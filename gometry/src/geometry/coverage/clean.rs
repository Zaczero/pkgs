use crate::error::Result;
use crate::geometry::coverage::{
    HashMap, coverage_rows, valid_coverage_rows, validate_polygonal_rows, visit_invalid_segments,
};
use crate::geometry::embedding_search::account::{EvaluationBudget, EvaluationOnly};
use crate::geometry::overlay::{ExactSourceInventory, overlay_declared_axes};
use crate::geometry::publication::{
    FinishedGeometry, ProducerReceipt, PublicationCall, PublicationOutcome, PublicationRejection,
    StorageTarget, finish_geographic,
};
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

pub(crate) enum CoverageCleanOutput {
    Shapes(Vec<Shape>),
    Finished(Box<FinishedGeometry>),
}

/// Clean a near-coverage and retain its final geographic storage receipt.
pub(crate) fn coverage_clean_output<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    grid_size: f64,
    gap_width: f64,
    overlaps: CoverageOverlapRule,
    context: &SourceArcContext<'_, '_>,
    output_frame: crate::Frame,
) -> Result<CoverageCleanOutput> {
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
    let declared_axes = snapped
        .iter()
        .fold(crate::geometry::CoordinateAxes::XY, |axes, shape| {
            axes.union(shape.axes())
        });

    match context.domain() {
        crate::geometry::TopologyDomain::Geographic(frame) => {
            validate_polygonal_rows(&snapped)?;
            let mut budget = EvaluationBudget::new("coverage_clean", 0)?;
            let mut work = EvaluationOnly::new(&mut budget);
            let analysis = crate::geometry::analyze_geographic_coverage_with_work(
                &snapped,
                frame,
                gap_width,
                "coverage_clean",
                &mut work,
            )?;
            let has_overlaps = analysis.has_overlapping_faces();
            let needs_area_ranks = has_overlaps
                && matches!(
                    overlaps,
                    CoverageOverlapRule::MaxArea | CoverageOverlapRule::MinArea
                );
            let needs_metric = needs_area_ranks
                || gap_width > 0.0
                || (has_overlaps && matches!(overlaps, CoverageOverlapRule::LongestBorder));
            let metric = if needs_metric {
                let metric_access = context.metric_access().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "geographic coverage ranking requires the operation's CRS metric".into(),
                    )
                })?;
                Some(metric_access()?)
            } else {
                None
            };
            let row_surface_areas = if needs_area_ranks {
                let metric = metric.ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "geographic area ranking is missing the operation's CRS metric".into(),
                    )
                })?;
                snapped
                    .iter()
                    .map(|shape| crate::crs::chart_shape_area(metric, shape, frame))
                    .collect::<Result<Vec<_>>>()?
            } else {
                vec![0.0; snapped.len()]
            };
            let selection = analysis.assign_cleaning_faces(
                &row_surface_areas,
                gap_width,
                overlaps,
                metric,
                &mut work,
            )?;
            let labels_preserve_sources =
                analysis.face_rows.iter().zip(&selection.owners).all(
                    |(source_rows, selected_row)| match source_rows.as_slice() {
                        [] => selected_row.is_none(),
                        [source_row] => selected_row == &Some(*source_row),
                        _ => false,
                    },
                ) && selection.gap_assignments.iter().all(|&assigned| !assigned);

            // Cleaning an already-valid coverage is an identity operation.
            // Preserve its original coordinate bits and ring presentation.
            if analysis.is_valid() && labels_preserve_sources {
                return Ok(CoverageCleanOutput::Shapes(snapped));
            }

            #[cfg(test)]
            drop(admission_phase);
            let receipt = analysis.coverage_drawing_receipt(
                &selection,
                &rows
                    .iter()
                    .map(|row| row.borrow().axes())
                    .collect::<Vec<_>>(),
                context,
                StorageTarget::Array,
                &mut work,
            )?;
            match finish_geographic(
                PublicationCall::new(
                    ProducerReceipt::ProducerDrawing(receipt),
                    output_frame,
                    &mut budget,
                ),
                StorageTarget::Array,
            )? {
                PublicationOutcome::Pass(finished) => {
                    Ok(CoverageCleanOutput::Finished(Box::new(finished)))
                },
                PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
                    Err(GeometryErrorKind::Invalid(
                        format!(
                            "final coverage-clean storage changed {} producer literal images",
                            images.len()
                        )
                        .into(),
                    )
                    .into())
                },
                PublicationOutcome::Reject(_) => Err(GeometryErrorKind::Invalid(
                    "coverage-clean finisher returned a different producer rejection family".into(),
                )
                .into()),
            }
        },
        crate::geometry::TopologyDomain::Cartesian => {
            let row_areas: Vec<_> = snapped.iter().map(Shape::area).collect();
            let prepared = coverage_rows(&snapped, gap_width)?;
            if !visit_invalid_segments(&prepared, gap_width, |_, _| true)? {
                return Ok(CoverageCleanOutput::Shapes(snapped));
            }
            #[cfg(test)]
            drop(admission_phase);
            #[cfg(test)]
            let source_staging_phase = crate::test_support::allocation_phase(
                crate::test_support::AllocationPhase::SourceStaging,
            );
            let shape_refs: Vec<_> = snapped.iter().collect();
            let inventory = ExactSourceInventory::from_shapes(&shape_refs)?;
            let mut budget =
                crate::geometry::ExpansionBudget::new("coverage_clean", "exact partition");
            let mut partition = inventory.partition(&mut budget)?;
            #[cfg(test)]
            drop(source_staging_phase);
            Ok(CoverageCleanOutput::Shapes(inventory.coverage_clean_rows(
                &mut partition,
                &row_areas,
                gap_width,
                overlaps,
                declared_axes,
                context,
            )?))
        },
    }
}

/// Clean a near-coverage into shapes for native callers.
///
/// Geographic public output consumes the `FinishedGeometry` directly so its
/// checked storage remains the object handed to Python.
#[cfg(test)]
pub(crate) fn coverage_clean<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    grid_size: f64,
    gap_width: f64,
    overlaps: CoverageOverlapRule,
    context: &SourceArcContext<'_, '_>,
) -> Result<Vec<Shape>> {
    match coverage_clean_output(
        rows,
        grid_size,
        gap_width,
        overlaps,
        context,
        crate::Frame::None,
    )? {
        CoverageCleanOutput::Shapes(shapes) => Ok(shapes),
        CoverageCleanOutput::Finished(finished) => {
            let (rows, _) = (*finished).into_array()?;
            rows.logical_rows()
                .map(|row| match row {
                    crate::geometry::storage::LogicalRow::Present { row, .. } => row.to_shape(),
                    crate::geometry::storage::LogicalRow::Missing { .. } => {
                        Err(GeometryErrorKind::Invalid(
                            "coverage-clean result unexpectedly contains a missing row".into(),
                        )
                        .into())
                    },
                })
                .collect()
        },
    }
}

/// Dissolve a valid geographic coverage from its original Q source roots when
/// raw bounds cross the longitude seam.
pub(crate) fn coverage_union_topo<S: std::borrow::Borrow<Shape>>(
    coverage: &[S],
    context: &SourceArcContext<'_, '_>,
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
    coverage_union_geographic(coverage, frame, context)
}

fn coverage_union_inner<S: std::borrow::Borrow<Shape>>(
    coverage: &[S],
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    // The fast dissolve is correct only for valid coverages. Validate at the
    // public kernel boundary instead of exposing an unchecked footgun.
    let prepared = valid_coverage_rows(coverage, "coverage_union_all")?;
    let source_refs: Vec<_> = coverage.iter().map(std::borrow::Borrow::borrow).collect();
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
        if context.domain().is_geographic() && !shape.is_empty() {
            return Ok(shape);
        }
        if context.domain().is_geographic() {
            return Ok(empty_coverage_union(&source_refs));
        }
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
        if context.domain().is_geographic() {
            let Some(first) = parts.first() else {
                return Ok(empty_coverage_union(&source_refs));
            };
            if !first.axes().has_z()
                && !first.axes().has_m()
                && parts.iter().all(|polygon| polygon.axes() == first.axes())
            {
                return Ok(Shape::narrow_polygons(first.axes(), parts));
            }
            return coverage_union_geographic_ordinary(&source_refs, context);
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
    if context.domain().is_geographic() {
        if !source_refs
            .iter()
            .any(|shape| shape.has_z() || shape.has_m())
        {
            return dissolve_edge_incidences(prepared.edge_incidences);
        }
        return coverage_union_geographic_ordinary(&source_refs, context);
    }
    carry_overlay_ordinates(
        dissolve_edge_incidences(prepared.edge_incidences)?,
        &source_refs,
        OverlayOp::Union,
        false,
        context,
    )
}

fn empty_coverage_union(source_refs: &[&Shape]) -> Shape {
    Shape::typed_empty(
        crate::geometry::GeometryKind::Polygon,
        overlay_declared_axes(source_refs, OverlayOp::Union),
    )
}

fn coverage_union_geographic_ordinary(
    source_refs: &[&Shape],
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    let crate::geometry::TopologyDomain::Geographic(_) = context.domain() else {
        unreachable!("the ordinary geographic dissolve requires its source frame")
    };
    let inventory = ExactSourceInventory::from_shapes(source_refs)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_union_all", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let declared_axes = source_refs
        .iter()
        .fold(crate::geometry::CoordinateAxes::XY, |axes, shape| {
            axes.union(shape.axes())
        });
    inventory.overlay_shape(&partition, OverlayOp::Union, declared_axes, false, context)
}

fn coverage_union_geographic<S: std::borrow::Borrow<Shape>>(
    coverage: &[S],
    frame: crate::geographic::GeographicFrame,
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    let mut budget = EvaluationBudget::new("coverage_union_all", 0)?;
    let mut work = EvaluationOnly::new(&mut budget);
    let analysis = crate::geometry::analyze_geographic_coverage_with_work(
        coverage,
        frame,
        0.0,
        "coverage_union_all",
        &mut work,
    )?;
    if !analysis.is_valid() {
        return Err(GeometryErrorKind::InvalidCoverage {
            operation: "coverage_union_all",
        }
        .into());
    }
    let row_areas = coverage
        .iter()
        .map(|shape| shape.borrow().area())
        .collect::<Vec<_>>();
    let mut assignment = analysis.assign_cleaning_faces(
        &row_areas,
        0.0,
        CoverageOverlapRule::MinIndex,
        None::<&crate::crs::GeodesicMetric>,
        &mut work,
    )?;
    // Valid coverage faces have at most one source owner. Assign all occupied
    // faces to the scalar result so Q cancels shared boundaries before writing.
    for owner in &mut assignment.owners {
        if owner.is_some() {
            *owner = Some(0);
        }
    }
    let receipt = analysis.coverage_drawing_receipt(
        &assignment,
        &coverage
            .iter()
            .map(|row| row.borrow().axes())
            .collect::<Vec<_>>(),
        context,
        StorageTarget::Scalar,
        &mut work,
    )?;
    match finish_geographic(
        PublicationCall::new(
            ProducerReceipt::ProducerDrawing(receipt),
            crate::Frame::None,
            &mut budget,
        ),
        StorageTarget::Scalar,
    )? {
        PublicationOutcome::Pass(finished) => finished.into_scalar().map(|(shape, _)| shape),
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            Err(GeometryErrorKind::Invalid(
                format!(
                    "final coverage-union storage changed {} producer literal images",
                    images.len()
                )
                .into(),
            )
            .into())
        },
        PublicationOutcome::Reject(_) => Err(GeometryErrorKind::Invalid(
            "coverage-union finisher returned a different producer rejection family".into(),
        )
        .into()),
    }
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
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_union_all", "exact partition");
    let mut partition = inventory.partition(&mut budget)?;
    let parts = inventory.linework_polygons(
        &mut partition,
        true,
        "coverage_union_all",
        crate::geometry::CoordinateAxes::XY,
        false,
        &SourceArcContext::cartesian(),
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
    use crate::geometry::{CoordinateAxes, Point, Polygon, Ring, XY, overlay};

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
            &SourceArcContext::cartesian(),
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
            &SourceArcContext::cartesian(),
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
            &SourceArcContext::cartesian(),
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
                &SourceArcContext::cartesian(),
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
                &SourceArcContext::cartesian(),
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

    // Separate roundings define the reproducible ULP jitter fixture.
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
                &SourceArcContext::cartesian(),
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
            &SourceArcContext::cartesian(),
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
        crate::geometry::coverage_union_topo(&cleaned, &SourceArcContext::cartesian())
            .expect("dissolve accepts the repaired jittered coverage");
    }

    #[test]
    fn geographic_coverage_union_keeps_ordinates_after_presplit_dissolve() -> Result<()> {
        let crs = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let first = crate::io::parse_wkt(
            "POLYGON ZM ((165 -20 1 10, -165 -10 11 110, \
             -165 30 21 210, 165 -20 1 10))",
        )?;
        let second = crate::io::parse_wkt(
            "POLYGON ZM ((165 -20 1 10, -165 30 21 210, \
             165 20 31 310, 165 -20 1 10))",
        )?;
        let rows = [first, second];
        let model = crate::crs::metric_model(Some(crs))?;
        let metric = crate::crs::GeodesicMetric::from_model(&model)?;
        let metric_access = || Ok(&metric);
        let context = SourceArcContext::new(
            crate::geometry::TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
            Some(&metric_access),
        );

        let forward = coverage_union_topo(&rows, &context)?;
        let reverse = coverage_union_topo(&[rows[1].clone(), rows[0].clone()], &context)?;

        for output in [&forward, &reverse] {
            if output.axes() != CoordinateAxes::XYZM {
                return Err(GeometryErrorKind::Invalid(
                    "presplit coverage dissolve dropped resolvable XYZM axes".into(),
                )
                .into());
            }
            let mut seam_samples = Vec::new();
            output.try_for_each_point(|point| {
                if point.x.abs().to_bits() == 180.0_f64.to_bits()
                    && point.y.to_bits() == (-15.0_f64).to_bits()
                {
                    seam_samples.push((point.z(), point.m()));
                }
                Ok::<_, crate::error::Error>(())
            })?;
            if seam_samples.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "presplit coverage dissolve lost its original seam-root sample".into(),
                )
                .into());
            }
            if seam_samples.iter().any(|(z, m)| z.is_none() || m.is_none()) {
                return Err(GeometryErrorKind::Invalid(
                    "presplit coverage dissolve emitted an unmeasured seam sample".into(),
                )
                .into());
            }
            let Some(first) = seam_samples.first() else {
                return Err(GeometryErrorKind::Invalid(
                    "presplit coverage dissolve has no seam sample".into(),
                )
                .into());
            };
            if seam_samples.iter().any(|sample| sample != first) {
                return Err(GeometryErrorKind::Invalid(
                    "reversed coverage owners disagree at the seam sample".into(),
                )
                .into());
            }
        }
        Ok(())
    }
}
