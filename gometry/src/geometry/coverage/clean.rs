use crate::error::Result;
use crate::geometry::coverage::{
    HashMap, coverage_rows, valid_coverage_rows, visit_invalid_segments,
};
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::{
    CoordSeq, GeometryErrorKind, OverlayOp, Shape, SourceArcContext, carry_overlay_ordinates,
    polygon_parts_to_shape,
};

// --- Cleaning ----------------------------------------------------------------

crate::tokens::token_enum! {
    /// How `coverage_clean` assigns a region covered by more than one row:
    /// to the candidate sharing the longest border with it, the largest or
    /// smallest candidate by area, or the lowest row index.
    pub(crate) enum CoverageOverlapRule("coverage_clean overlaps", token = none, param = "overlap_rule") {
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
    context: SourceArcContext<'_, '_>,
) -> Result<Vec<Shape>> {
    if !(grid_size.is_finite() && grid_size >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("grid_size", grid_size).into());
    }
    #[cfg(test)]
    let admission_phase =
        crate::test_support::allocation_phase(crate::test_support::AllocationPhase::Admission);
    let mut snapped = Vec::with_capacity(rows.len());
    if grid_size > 0.0 {
        for row in rows {
            snapped.push(row.borrow().snap_to_grid_repaired(
                (grid_size, grid_size),
                (0.0, 0.0),
                crate::geometry::TopologyDomain::Cartesian,
                context,
            )?);
        }
    } else {
        snapped.extend(rows.iter().map(|row| row.borrow().clone()));
    }
    let prepared = coverage_rows(&snapped, gap_width)?;
    // Cleaning an already-valid coverage is an identity operation. Besides
    // avoiding needless partition work, this preserves coordinate bits, ring
    // presentation, and makes repeated cleaning exactly idempotent.
    // Finish validation so errors from later rows precede partition construction.
    if !visit_invalid_segments(&prepared, gap_width, |_, _| true)? {
        return Ok(snapped);
    }
    drop(prepared);

    #[cfg(test)]
    drop(admission_phase);
    #[cfg(test)]
    let source_staging_phase =
        crate::test_support::allocation_phase(crate::test_support::AllocationPhase::SourceStaging);
    let shape_refs: Vec<_> = snapped.iter().collect();
    let inventory = ExactSourceInventory::from_shapes(&shape_refs)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_clean", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    #[cfg(test)]
    drop(source_staging_phase);
    let row_areas: Vec<_> = snapped.iter().map(Shape::area).collect();
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
        context,
    )
}

/// Frame-aware coverage dissolve: rows whose raw boxes are unsound in the
/// geographic frame are split before the edge inventory sees them as planar.
pub(crate) fn coverage_union_topo<S: std::borrow::Borrow<Shape>>(
    coverage: &[S],
    context: SourceArcContext<'_, '_>,
) -> Result<Shape> {
    let domain = context.domain();
    let crate::geometry::TopologyDomain::Geographic(frame) = domain else {
        return coverage_union_inner(coverage, context);
    };
    if coverage.iter().all(|row| {
        let shape = row.borrow();
        shape.bounds().is_some_and(|bounds| {
            crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(shape), bounds, frame)
        })
    }) {
        return coverage_union_inner(coverage, context);
    }
    let mut split_owned = Vec::with_capacity(coverage.len());
    for row in coverage {
        let shape = row.borrow();
        let split = if shape.bounds().is_some_and(|bounds| {
            crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(shape), bounds, frame)
        }) {
            shape.clone()
        } else {
            shape.split_antimeridian_in_frame_with_metric(frame, context.metric_access())?
        };
        split_owned.push(split);
    }
    coverage_union_inner(&split_owned, context)
}

fn coverage_union_inner<S: std::borrow::Borrow<Shape>>(
    coverage: &[S],
    context: SourceArcContext<'_, '_>,
) -> Result<Shape> {
    // The fast dissolve is correct only for valid coverages. Validate at the
    // public kernel boundary instead of exposing an unchecked footgun.
    let prepared = valid_coverage_rows(coverage, "coverage_union")?;
    let source_refs: Vec<_> = coverage.iter().map(|shape| shape.borrow()).collect();
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
                let Some(polygon) = polygons.first() else {
                    continue;
                };
                if polygons.len() > 1 || sole.is_some() {
                    multi = true;
                    break;
                }
                sole = Some(polygon);
            },
            // Empties contribute no regions; non-polygonal rows are rejected by
            // coverage_rows before we get here.
            Shape::Empty(..) => {},
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::GeometryCollection(_) => {
                unreachable!("coverage rows admit only polygonal shapes")
            },
        }
    }
    if !multi {
        let shape = sole.map_or_else(Shape::empty_polygon, |polygon| {
            Shape::Polygon(polygon.clone())
        });
        return carry_overlay_ordinates(shape, &source_refs, OverlayOp::Union, false, context);
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
        let mut parts = Vec::with_capacity(prepared.rows.len());
        for row in &prepared.rows {
            match row.shape {
                Shape::Polygon(polygon) => parts.push(polygon.clone()),
                Shape::MultiPolygon(polygons) => {
                    parts.extend(polygons.iter().cloned());
                },
                Shape::Empty(..) => {},
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::GeometryCollection(_) => {
                    unreachable!("coverage rows admit only polygonal shapes")
                },
            }
        }
        // The direct path already proves these parts are the complete union.
        // Make only its temporary payload homogeneous; keep source_refs intact
        // so carry_overlay_ordinates remains the owner of ordinate retention.
        let parts = if parts.first().is_some_and(|first| {
            let axes = first.axes();
            parts.iter().any(|polygon| polygon.axes() != axes)
        }) {
            let mut homogeneous = Vec::with_capacity(parts.len());
            for polygon in parts {
                homogeneous.push(polygon.map_ring_seqs(CoordSeq::force_2d)?);
            }
            homogeneous
        } else {
            parts
        };
        return carry_overlay_ordinates(
            polygon_parts_to_shape(parts),
            &source_refs,
            OverlayOp::Union,
            false,
            context,
        );
    }
    carry_overlay_ordinates(
        dissolve_edge_incidences(prepared.edge_incidences)?,
        &source_refs,
        OverlayOp::Union,
        false,
        context,
    )
}

/// Dissolve the validation product's exterior incidences through one exact
/// partition. Coverage cleaning does not use this path: its row outputs are
/// published directly from the labeled source partition above.
fn dissolve_edge_incidences(
    incidences: HashMap<
        (crate::geometry::PointKey, crate::geometry::PointKey),
        (crate::geometry::Segment, usize),
    >,
) -> Result<Shape> {
    let mut survivors = Vec::with_capacity(incidences.len());
    for (segment, count) in incidences.into_values() {
        if count == 1 {
            survivors.push(segment);
        }
    }
    if survivors.is_empty() {
        return Ok(Shape::empty_polygon());
    }
    survivors
        .sort_unstable_by_key(|segment| crate::geometry::undirected_segment_edge_key(*segment));
    let inventory = ExactSourceInventory::from_segments(&survivors)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_union", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    let parts = inventory.linework_polygons(
        &mut partition,
        true,
        "coverage_union",
        crate::geometry::CoordinateAxes::XY,
        false,
        SourceArcContext::cartesian(),
    )?;
    let polygons = parts
        .into_iter()
        .map(|part| {
            let Shape::Polygon(polygon) = part else {
                unreachable!("coverage union linework emits only polygons")
            };
            polygon
        })
        .collect();
    Ok(polygon_parts_to_shape(polygons))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{Point, Polygon, Ring, XY, overlay};

    fn rectangle_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
        let points = [
            Point::new_raw_xy(minx, miny),
            Point::new_raw_xy(maxx, miny),
            Point::new_raw_xy(maxx, maxy),
            Point::new_raw_xy(minx, maxy),
            Point::new_raw_xy(minx, miny),
        ];
        Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        )
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
        let point_capacity = subdivisions
            .checked_mul(4)
            .and_then(|count| count.checked_add(1))
            .expect("test rectangle point count fits usize");
        let mut points = Vec::with_capacity(point_capacity);
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
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn dense_rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        dense_rectangle_with_subdivisions(minx, miny, maxx, maxy, 256)
    }

    #[test]
    fn coverage_clean_builds_one_partition_after_admission() {
        let rows = [rectangle(0.0, 0.0, 1.2, 1.0), rectangle(1.0, 0.0, 2.0, 1.0)];
        overlay::reset_partition_builds();
        coverage_clean(
            &rows,
            0.0,
            0.0,
            CoverageOverlapRule::LongestBorder,
            SourceArcContext::cartesian(),
        )
        .expect("coverage clean");
        assert_eq!(overlay::partition_builds(), 1);
    }

    #[test]
    fn coverage_clean_longest_border_orders_overflowed_finite_borders() {
        let rows = [
            rectangle(-1.6e308, -1.7e308, 1.6e308, 1.7e308),
            rectangle(-1.7e308, 0.0, 1.7e308, 1.0e308),
        ];
        let output = coverage_clean(
            &rows,
            0.0,
            0.0,
            CoverageOverlapRule::LongestBorder,
            SourceArcContext::cartesian(),
        )
        .expect("finite-coordinate overlap");
        let overlap = Point::new_raw_xy(0.0, 0.5e308);
        assert_eq!(
            crate::geometry::cartesian_point_location(&output[0], overlap),
            crate::geometry::Loc::Exterior
        );
        assert_eq!(
            crate::geometry::cartesian_point_location(&output[1], overlap),
            crate::geometry::Loc::Interior
        );
    }

    #[test]
    fn coverage_clean_keeps_one_shared_node_coordinate_for_both_rows() {
        let rows = [rectangle(0.0, 0.0, 1.2, 1.0), rectangle(1.0, 0.0, 2.0, 1.0)];
        let output = coverage_clean(
            &rows,
            0.0,
            0.0,
            CoverageOverlapRule::LongestBorder,
            SourceArcContext::cartesian(),
        )
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
        overlay::reset_partition_builds();
        let (output, allocations) = crate::test_support::allocation_count(|| {
            coverage_clean(
                &rows,
                0.0,
                0.0,
                CoverageOverlapRule::LongestBorder,
                SourceArcContext::cartesian(),
            )
            .expect("dense collinear coverage clean")
        });
        let metrics = overlay::partition_metrics();
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
    fn coverage_clean_preserves_reversed_duplicate_and_cancelled_collinear_support() {
        let other = rectangle(2.0, 0.0, 6.0, 4.0);
        for wkt in [
            "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
            "POLYGON ((0 0, 2 0, 4 0, 4 4, 0 4, 0 0))",
            "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0, -2 0, 0 0))",
        ] {
            let left = crate::io::parse_wkt(wkt).expect("collinear traversal fixture");
            let rows = [left, other.clone()];
            let output = coverage_clean(
                &rows,
                0.0,
                0.0,
                CoverageOverlapRule::LongestBorder,
                SourceArcContext::cartesian(),
            )
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

    #[expect(
        clippy::suboptimal_flops,
        reason = "separate roundings define the reproducible ULP jitter fixture"
    )]
    fn ulp_jitter(value: f64, seed: usize) -> f64 {
        let magnitude = value.abs().max(1.0);
        let spacing = f64::from_bits(magnitude.to_bits().wrapping_add(1)) - magnitude;
        let offset = match seed.wrapping_mul(17).wrapping_add(5) % 7 {
            0 => -3,
            1 => -2,
            2 => -1,
            3 => 0,
            4 => 1,
            5 => 2,
            _ => 3,
        };
        value + f64::from(offset) * spacing
    }

    fn jittered_grid(mode: &str) -> Vec<Shape> {
        (0_u8..12)
            .flat_map(|row| {
                (0_u8..12).map(move |column| {
                    if mode == "exact" {
                        return rectangle(
                            f64::from(column),
                            f64::from(row),
                            f64::from(column.wrapping_add(1)),
                            f64::from(row.wrapping_add(1)),
                        );
                    }
                    if mode == "coherent" {
                        let point = |vertex_row: u8, vertex_column: u8| {
                            let key = usize::from(vertex_row)
                                .wrapping_mul(13)
                                .wrapping_add(usize::from(vertex_column));
                            XY::new(
                                ulp_jitter(f64::from(vertex_column), key),
                                ulp_jitter(f64::from(vertex_row), key.wrapping_add(1)),
                            )
                        };
                        let points = [
                            point(row, column),
                            point(row, column.wrapping_add(1)),
                            point(row.wrapping_add(1), column.wrapping_add(1)),
                            point(row.wrapping_add(1), column),
                            point(row, column),
                        ];
                        return Shape::Polygon(Polygon::new(
                            Ring::from_closed(CoordSeq::try_from_xy(&points).unwrap()).unwrap(),
                            Vec::new(),
                        ));
                    }

                    let seed = usize::from(row)
                        .wrapping_mul(12)
                        .wrapping_add(usize::from(column));
                    rectangle(
                        ulp_jitter(f64::from(column), seed),
                        ulp_jitter(f64::from(row), seed.wrapping_add(1)),
                        ulp_jitter(f64::from(column.wrapping_add(1)), seed.wrapping_add(2)),
                        ulp_jitter(f64::from(row.wrapping_add(1)), seed.wrapping_add(3)),
                    )
                })
            })
            .collect()
    }

    #[test]
    fn coverage_clean_repairs_jittered_grid_without_changing_valid_controls() {
        for mode in ["exact", "coherent"] {
            let rows = jittered_grid(mode);
            assert!(
                crate::geometry::coverage_is_valid(&rows, 0.0).unwrap(),
                "{mode}"
            );
            let cleaned = coverage_clean(
                &rows,
                0.0,
                0.0,
                CoverageOverlapRule::LongestBorder,
                SourceArcContext::cartesian(),
            )
            .unwrap();
            assert_eq!(cleaned, rows, "{mode}");
            assert!(
                crate::geometry::coverage_invalid_edges(&cleaned, 0.0)
                    .unwrap()
                    .iter()
                    .all(Shape::is_empty),
                "{mode}"
            );
        }

        let rows = jittered_grid("independent");
        assert!(!crate::geometry::coverage_is_valid(&rows, 0.0).unwrap());
        let cleaned = coverage_clean(
            &rows,
            0.0,
            0.0,
            CoverageOverlapRule::LongestBorder,
            SourceArcContext::cartesian(),
        )
        .unwrap();
        assert_eq!(cleaned.len(), 144);
        assert!(crate::geometry::coverage_is_valid(&cleaned, 0.0).unwrap());
        assert!(
            crate::geometry::coverage_invalid_edges(&cleaned, 0.0)
                .unwrap()
                .iter()
                .all(Shape::is_empty)
        );
        crate::geometry::coverage_union_topo(&cleaned, SourceArcContext::cartesian())
            .expect("dissolve accepts the repaired jittered coverage");
    }
}
