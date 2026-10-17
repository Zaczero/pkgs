use std::num::NonZeroUsize;
use std::ops::ControlFlow;

use super::certificate::LocalProjectionCertificate;
use super::frame::{LocalCrsDefinition, estimate_local_crs_at, local_crs_fits_at};
use crate::crs::{RuntimeState, geodesic_for_crs_at, transform_at};
use crate::error::Result;
use crate::geometry::{
    ChartSourceParameters, CoordSeq, ExpansionBudget, RefinedChainProvenance, RunIdentity,
    SegmentPlacement, Shape, line_segments, segmentize_points_with_piece_counts_budgeted,
};

const LOCAL_INPUT_REFINEMENT_FRACTION: f64 = 1.0e-4;

fn local_projection_refinement_budget(
    frame_extent_m: f64,
    operation_distance_m: Option<f64>,
) -> f64 {
    let extent = operation_distance_m.map_or(frame_extent_m, |distance| {
        frame_extent_m.min(distance.abs())
    });
    extent * LOCAL_INPUT_REFINEMENT_FRACTION
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalInputPurpose {
    BoundarySupport,
    ClearanceWitness,
    OriginalSites,
}

/// One selected geographic frame and its prepared projected inputs.
///
/// `projected_original` preserves the input's vertex sites. When chart-edge
/// refinement is needed, `projected_working` additionally contains equal-
/// arclength samples for boundary and edge-support operations.
pub(crate) struct PreparedLocalInput {
    local: LocalCrsDefinition,
    frame: crate::geographic::GeographicFrame,
    /// First-occurrence members when the source contains repeated shared subtrees.
    source_members: Option<Shape>,
    projected_original: Shape,
    /// Source-chart sequences with inserted samples where refinement was needed.
    source_refined: Option<Shape>,
    projected_refined: Option<Shape>,
    /// Edge lineage retained only for refined chains used by minimum clearance.
    refined_chains: Vec<RefinedChainProvenance>,
}

impl PreparedLocalInput {
    /// Select, project, and conditionally refine one non-empty geographic input.
    ///
    /// The original projection supplies the extent budget and remains the
    /// working shape when the arithmetic screen finds no unsafe chart edge.
    pub(crate) fn prepare(
        shape: &Shape,
        fitting_shapes: &[&Shape],
        source_crs: &str,
        operation_distance_m: Option<f64>,
        purpose: LocalInputPurpose,
        state: &RuntimeState,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<Self>> {
        if shape.is_empty() {
            return Ok(None);
        }

        let local = Self::select_local_frame(fitting_shapes, source_crs, state)?;
        Self::prepare_selected(
            shape,
            local,
            source_crs,
            operation_distance_m,
            purpose,
            state,
            budget,
        )
    }

    /// Select one datum-preserving local frame for the shapes that must fit it.
    pub(super) fn select_local_frame(
        fitting_shapes: &[&Shape],
        source_crs: &str,
        state: &RuntimeState,
    ) -> Result<LocalCrsDefinition> {
        estimate_local_crs_at(fitting_shapes, source_crs, state)
    }

    /// Prepare one source shape in a frame previously selected by this owner.
    ///
    /// The fitting shapes can include a prior result during the single
    /// constructive refit, while the refinement budget remains based on the
    /// original source shape's projected extent.
    pub(super) fn prepare_selected(
        shape: &Shape,
        local: LocalCrsDefinition,
        source_crs: &str,
        operation_distance_m: Option<f64>,
        purpose: LocalInputPurpose,
        state: &RuntimeState,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<Self>> {
        if shape.is_empty() {
            return Ok(None);
        }

        let source_members =
            if matches!(shape, Shape::GeometryCollection(_)) && shape.has_repeated_shared_owner() {
                let mut members = Vec::new();
                shape.try_for_each_first_component(&mut |component| {
                    members.push(component.to_shape());
                    Ok::<_, crate::error::Error>(())
                })?;
                Some(Shape::geometry_collection(members)?)
            } else {
                None
            };
        let shape = source_members.as_ref().unwrap_or(shape);
        let transform = crate::crs::TransformPlan::new_with_options_at(
            source_crs,
            &local.definition,
            crate::crs::TransformOptions::default(),
            std::sync::Arc::new(state.clone()),
        )
        .map_err(|error| local.annotate_error(error))?;
        let projected_original = transform
            .transform_shape(shape)
            .map_err(|error| local.annotate_error(error))?;
        let frame = transform.source_geographic_frame().ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "local projection input has no geographic coordinate frame",
            )
        })?;
        drop(transform);
        if purpose == LocalInputPurpose::OriginalSites {
            return Ok(Some(Self {
                local,
                frame,
                source_members,
                projected_original,
                source_refined: None,
                projected_refined: None,
                refined_chains: Vec::new(),
            }));
        }

        let geodesic = geodesic_for_crs_at(source_crs, state)?;
        let certificate =
            LocalProjectionCertificate::from_selected_frame(&local, &geodesic, frame, state)?;
        let extent_m = projected_original.bounds().map_or(0.0, |bounds| {
            (bounds.maxx() - bounds.minx()).hypot(bounds.maxy() - bounds.miny())
        });
        let refinement_budget = local_projection_refinement_budget(extent_m, operation_distance_m);

        let screen: ControlFlow<Result<bool>> =
            shape.for_each_first_segment_chain(RunIdentity::XY, |chain| {
                match certificate.has_edges_to_refine(chain, refinement_budget) {
                    Ok(true) => ControlFlow::Break(Ok(true)),
                    Ok(false) => ControlFlow::Continue(()),
                    Err(error) => ControlFlow::Break(Err(error)),
                }
            });
        let needs_refinement = match screen {
            ControlFlow::Continue(()) => false,
            ControlFlow::Break(Ok(needs_refinement)) => needs_refinement,
            ControlFlow::Break(Err(error)) => return Err(error),
        };
        if !needs_refinement {
            return Ok(Some(Self {
                local,
                frame,
                source_members,
                projected_original,
                source_refined: None,
                projected_refined: None,
                refined_chains: Vec::new(),
            }));
        }

        let mut refined_any_chain = false;
        let mut chain_index = 0_usize;
        let mut refined_chains = Vec::new();
        let mut refine_chain = |chain: &crate::geometry::CoordSeq| {
            let current_chain = chain_index;
            chain_index = chain_index
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            if !certificate.has_edges_to_refine(chain, refinement_budget)? {
                return Ok(None);
            }

            let mut piece_counts =
                Vec::<NonZeroUsize>::with_capacity(chain.len().saturating_sub(1));
            for segment in line_segments(chain) {
                piece_counts.push(certificate.piece_count(
                    crate::geometry::Point::new_raw_xy(segment.start.x, segment.start.y),
                    crate::geometry::Point::new_raw_xy(segment.end.x, segment.end.y),
                    refinement_budget,
                )?);
            }
            if piece_counts.iter().all(|pieces| pieces.get() == 1) {
                return Ok(None);
            }

            refined_any_chain = true;
            let retain_source_parameters = purpose == LocalInputPurpose::ClearanceWitness
                && (chain.axes().has_z() || chain.axes().has_m());
            let (refined, edge_ends, source_parameters) = if retain_source_parameters {
                let (refined, ranges): (CoordSeq, ChartSourceParameters) =
                    crate::geometry::segmentize_chart_points_with_piece_counts_budgeted(
                        chain,
                        &piece_counts,
                        &geodesic,
                        frame,
                        budget,
                    )?;
                (refined, ranges.edge_ends, Some(ranges.parameters))
            } else {
                let refined = segmentize_points_with_piece_counts_budgeted(
                    chain,
                    &piece_counts,
                    &SegmentPlacement::Chart(&geodesic, frame),
                    budget,
                )?;
                let mut piece_ends = Vec::with_capacity(piece_counts.len());
                let mut end = 0_usize;
                for pieces in &piece_counts {
                    end = end
                        .checked_add(pieces.get())
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    piece_ends.push(end);
                }
                (refined, piece_ends, None)
            };
            let provenance = match source_parameters {
                Some(source_parameters) => RefinedChainProvenance::from_chart_parameters(
                    current_chain,
                    &edge_ends,
                    source_parameters,
                )?,
                None => RefinedChainProvenance {
                    chain_index: current_chain,
                    piece_ends: edge_ends,
                    source_parameters: None,
                },
            };
            refined_chains.push(provenance);
            Ok(Some(refined))
        };
        let refined_source = shape.map_chains(&mut refine_chain)?;
        let (source_refined, projected_refined) = if refined_any_chain {
            let projected = transform_at(&refined_source, source_crs, &local.definition, state)
                .map_err(|error| local.annotate_error(error))?;
            (Some(refined_source), Some(projected))
        } else {
            (None, None)
        };

        Ok(Some(Self {
            local,
            frame,
            source_members,
            projected_original,
            source_refined,
            projected_refined,
            refined_chains,
        }))
    }

    pub(super) const fn local_crs(&self) -> &LocalCrsDefinition {
        &self.local
    }

    pub(super) const fn geographic_frame(&self) -> crate::geographic::GeographicFrame {
        self.frame
    }

    /// Original projected vertices for site and provenance semantics.
    pub(super) const fn projected_original(&self) -> &Shape {
        &self.projected_original
    }

    pub(super) fn source_original<'a>(&'a self, original: &'a Shape) -> &'a Shape {
        self.source_members.as_ref().unwrap_or(original)
    }

    /// Projected input with certified chart-edge refinement for boundary work.
    pub(crate) fn projected_working(&self) -> &Shape {
        self.projected_refined
            .as_ref()
            .unwrap_or(&self.projected_original)
    }

    pub(super) fn source_working<'a>(&'a self, original: &'a Shape) -> &'a Shape {
        self.source_refined
            .as_ref()
            .unwrap_or_else(|| self.source_original(original))
    }

    pub(super) fn refined_chains(&self) -> &[RefinedChainProvenance] {
        &self.refined_chains
    }

    pub(super) fn restored_result_fits(
        &self,
        result: &Shape,
        source_crs: &str,
        state: &RuntimeState,
    ) -> Result<bool> {
        local_crs_fits_at(result, source_crs, &self.local.definition, state)
    }
}

#[cfg(test)]
mod tests {
    use super::super::operations::geodesic_by_identity;
    use super::*;
    use crate::crs::runtime_state_snapshot;
    use crate::geometry::{Coordinates as _, LineSeq, XY};

    fn sampled_projection_deviation(
        source_crs: &str,
        target_crs: &str,
        start: XY,
        end: XY,
        state: &RuntimeState,
    ) -> f64 {
        let samples = (0..=4096)
            .map(|step| {
                let fraction = f64::from(step) / 4096.0;
                XY::new(
                    start.x + fraction * (end.x - start.x),
                    start.y + fraction * (end.y - start.y),
                )
            })
            .collect::<Vec<_>>();
        let sample_shape =
            Shape::LineString(LineSeq::try_new(CoordSeq::try_from_xy(&samples).unwrap()).unwrap());
        let projected =
            crate::crs::transform_at(&sample_shape, source_crs, target_crs, state).unwrap();
        let projected = projected.points_vec().unwrap();
        let start = projected.first().unwrap();
        let end = projected.last().unwrap();
        let dx = end.x - start.x;
        let dy = end.y - start.y;
        let length_squared = dx * dx + dy * dy;
        projected.iter().fold(0.0_f64, |maximum, point| {
            let fraction = (((point.x - start.x) * dx + (point.y - start.y) * dy) / length_squared)
                .clamp(0.0, 1.0);
            maximum.max(
                (point.x - (start.x + fraction * dx)).hypot(point.y - (start.y + fraction * dy)),
            )
        })
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn safe_geographic_edges_reuse_the_original_projected_shape() {
        let points =
            CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(0.1, 0.0), XY::new(0.2, 0.0)])
                .unwrap();
        let shape = Shape::LineString(LineSeq::try_new(points).unwrap());
        let state = runtime_state_snapshot().unwrap();
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");

        let prepared = PreparedLocalInput::prepare(
            &shape,
            &[&shape],
            "EPSG:4326",
            Some(1_000.0),
            LocalInputPurpose::BoundarySupport,
            &state,
            &mut budget,
        )
        .unwrap()
        .unwrap();

        let (Shape::LineString(original), Shape::LineString(working)) =
            (prepared.projected_original(), prepared.projected_working())
        else {
            panic!("line input remains a line");
        };
        assert_eq!(original.len(), 3);
        assert_eq!(working.len(), 3);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn failed_geographic_edge_uses_chart_counts_and_the_callers_budget() {
        let west = -0.463_147_336_714_631_5;
        let east = 6.463_147_336_714_631_5;
        let points = CoordSeq::try_from_xy(&[XY::new(west, 39.0), XY::new(east, 39.0)]).unwrap();
        let shape = Shape::LineString(LineSeq::try_new(points).unwrap());
        let state = runtime_state_snapshot().unwrap();
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");

        let prepared = PreparedLocalInput::prepare(
            &shape,
            &[&shape],
            "EPSG:4326",
            Some(1_000.0),
            LocalInputPurpose::BoundarySupport,
            &state,
            &mut budget,
        )
        .unwrap()
        .unwrap();

        let Shape::LineString(working) = prepared.projected_working() else {
            panic!("line input remains a line");
        };
        assert_eq!(working.len(), 254);
        assert_eq!(budget.used(), 252);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn refinement_lineage_preserves_safe_and_repeated_collection_chains() {
        let safe = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_xy(&[XY::new(3.0, 39.2), XY::new(3.001, 39.2)]).unwrap(),
            )
            .unwrap(),
        );
        let long_edge = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_xy(&[
                    XY::new(-0.463_147_336_714_631_5, 39.0),
                    XY::new(6.463_147_336_714_631_5, 39.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let shape = Shape::geometry_collection(vec![safe, long_edge.clone(), long_edge]).unwrap();
        assert!(shape.has_repeated_shared_owner());
        let state = runtime_state_snapshot().unwrap();
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");

        let prepared = PreparedLocalInput::prepare(
            &shape,
            &[&shape],
            "EPSG:4326",
            Some(1_000.0),
            LocalInputPurpose::BoundarySupport,
            &state,
            &mut budget,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            prepared
                .refined_chains()
                .iter()
                .map(|lineage| lineage.chain_index)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        let source_count = crate::geometry::ShapeCountQuery::default()
            .coordinates(&shape)
            .unwrap();
        let refined_count = crate::geometry::ShapeCountQuery::default()
            .coordinates(prepared.source_working(&shape))
            .unwrap();
        assert_eq!(budget.used(), refined_count - source_count);
        assert!(refined_count > source_count);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn selected_projection_pieces_stay_within_budget_for_each_family() {
        const SAMPLES_PER_PIECE: usize = 129;

        let state = runtime_state_snapshot().unwrap();
        let cases = [
            (
                "EPSG:4326",
                XY::new(6.0, 0.0),
                XY::new(6.0, 1.0),
                None,
                Some("EPSG:32632"),
            ),
            (
                "EPSG:4326",
                XY::new(-0.463_147_336_714_631_5, 39.0),
                XY::new(6.463_147_336_714_631_5, 39.0),
                Some(1_000.0),
                Some("EPSG:32631"),
            ),
            (
                "EPSG:4326",
                XY::new(-0.463_147_336_714_631_5, 39.0),
                XY::new(6.463_147_336_714_631_5, 39.0),
                Some(10_000.0),
                Some("EPSG:32631"),
            ),
            (
                "EPSG:4326",
                XY::new(-3.028_618_285_290_685, -81.0),
                XY::new(-2.971_381_714_709_315, -81.0),
                None,
                Some("EPSG:5042"),
            ),
            (
                "EPSG:4326",
                XY::new(-0.05, 89.0),
                XY::new(0.05, 89.0),
                None,
                None,
            ),
            (
                "EPSG:4047",
                XY::new(-0.2, -0.1),
                XY::new(0.2, 0.2),
                None,
                None,
            ),
        ];
        for (source_crs, start, end, distance, expected_authority) in cases {
            let shape = Shape::LineString(
                LineSeq::try_new(crate::geometry::CoordSeq::try_from_xy(&[start, end]).unwrap())
                    .unwrap(),
            );
            let mut budget = ExpansionBudget::new("local_fidelity_test", "input_refinement");
            let prepared = PreparedLocalInput::prepare(
                &shape,
                &[&shape],
                source_crs,
                distance,
                LocalInputPurpose::BoundarySupport,
                &state,
                &mut budget,
            )
            .unwrap()
            .unwrap();
            if let Some(expected) = expected_authority {
                assert_eq!(prepared.local_crs().definition, expected);
            } else {
                assert!(
                    prepared.local_crs().definition.starts_with('{'),
                    "{source_crs} selected a custom local frame",
                );
            }

            let extent_m = prepared
                .projected_original()
                .bounds()
                .map_or(0.0, |bounds| {
                    (bounds.maxx() - bounds.minx()).hypot(bounds.maxy() - bounds.miny())
                });
            let piece_budget = super::local_projection_refinement_budget(extent_m, distance);
            let Shape::LineString(source_pieces) = prepared.source_working(&shape) else {
                panic!("line source remains a line");
            };
            if source_crs == "EPSG:4326"
                && start.x.to_bits() == (-0.463_147_336_714_631_5_f64).to_bits()
                && start.y.to_bits() == 39.0_f64.to_bits()
                && end.y.to_bits() == 39.0_f64.to_bits()
            {
                assert!(
                    sampled_projection_deviation(
                        source_crs,
                        &prepared.local_crs().definition,
                        start,
                        end,
                        &state,
                    ) > piece_budget,
                    "bypassing input refinement must exceed the 600 km parallel budget",
                );
                assert!(source_pieces.len() > 2);
            }
            let piece_points: Vec<_> = source_pieces.iter_coords().collect();
            let mut samples = Vec::with_capacity(
                piece_points
                    .len()
                    .saturating_sub(1)
                    .saturating_mul(SAMPLES_PER_PIECE),
            );
            for pair in piece_points.windows(2) {
                let [piece_start, piece_end] = pair else {
                    unreachable!("a coordinate window contains two points");
                };
                for step in 0..SAMPLES_PER_PIECE {
                    let fraction = step as f64 / (SAMPLES_PER_PIECE - 1) as f64;
                    samples.push(XY::new(
                        piece_start.x + fraction * (piece_end.x - piece_start.x),
                        piece_start.y + fraction * (piece_end.y - piece_start.y),
                    ));
                }
            }
            let sample_shape = Shape::LineString(
                LineSeq::try_new(crate::geometry::CoordSeq::try_from_xy(&samples).unwrap())
                    .unwrap(),
            );
            let projected_samples = crate::crs::transform_at(
                &sample_shape,
                source_crs,
                &prepared.local_crs().definition,
                &state,
            )
            .unwrap();
            let projected_points = projected_samples.points_vec().unwrap();
            let mut maximum_deviation: f64 = 0.0;
            let (projected_pieces, remainder) = projected_points.as_chunks::<SAMPLES_PER_PIECE>();
            assert!(remainder.is_empty());
            for piece in projected_pieces {
                let first = piece.first().expect("sample array has a first point");
                let last = piece.last().expect("sample array has a last point");
                let dx = last.x - first.x;
                let dy = last.y - first.y;
                let length_squared = dx * dx + dy * dy;
                for point in piece {
                    let fraction = (((point.x - first.x) * dx + (point.y - first.y) * dy)
                        / length_squared)
                        .clamp(0.0, 1.0);
                    maximum_deviation = maximum_deviation.max(
                        (point.x - (first.x + fraction * dx))
                            .hypot(point.y - (first.y + fraction * dy)),
                    );
                }
            }
            assert!(
                maximum_deviation <= piece_budget + 1.0e-7,
                "{source_crs} → {} exceeded {piece_budget} m with {maximum_deviation} m",
                prepared.local_crs().definition,
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn using_an_unselected_zone_certificate_misses_meridian_curvature() {
        let source = Shape::LineString(
            LineSeq::try_new(
                crate::geometry::CoordSeq::try_from_xy(&[XY::new(3.0, 59.9), XY::new(3.0, 60.1)])
                    .unwrap(),
            )
            .unwrap(),
        );
        let state = runtime_state_snapshot().unwrap();
        let selected =
            PreparedLocalInput::select_local_frame(&[&source], "EPSG:4326", &state).unwrap();
        assert_eq!(selected.definition, "EPSG:32632");

        let frame = crate::crs::info_with_state("EPSG:4326", &state)
            .unwrap()
            .geographic_frame()
            .unwrap();
        let geodesic = geodesic_for_crs_at("EPSG:4326", &state).unwrap();
        let unselected = LocalCrsDefinition {
            definition: "EPSG:32631".to_owned(),
            display_name: None,
            projected_pole: None,
        };
        let wrong_certificate =
            LocalProjectionCertificate::from_selected_frame(&unselected, &geodesic, frame, &state)
                .unwrap();
        let wrong_count = wrong_certificate
            .piece_count(
                crate::geometry::Point::new_raw_xy(3.0, 59.9),
                crate::geometry::Point::new_raw_xy(3.0, 60.1),
                1.0e-4,
            )
            .unwrap();
        assert_eq!(wrong_count.get(), 1);

        let maximum_deviation = sampled_projection_deviation(
            "EPSG:4326",
            &selected.definition,
            XY::new(3.0, 59.9),
            XY::new(3.0, 60.1),
            &state,
        );
        assert!(maximum_deviation > 1.0e-4, "{maximum_deviation} m");
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn parallel_only_estimate_misses_the_utm32_meridian_curve() {
        let source = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_xy(&[XY::new(3.0, 59.9), XY::new(3.0, 60.1)]).unwrap(),
            )
            .unwrap(),
        );
        let state = runtime_state_snapshot().unwrap();
        let selected =
            PreparedLocalInput::select_local_frame(&[&source], "EPSG:4326", &state).unwrap();
        assert_eq!(selected.definition, "EPSG:32632");

        // The old parallel-only model measured only the source parallel chord;
        // a meridian has no longitude sweep and the model returns zero.
        let longitude_delta = 3.0_f64 - 3.0;
        let midpoint_latitude = f64::midpoint(59.9, 60.1).to_radians();
        let legacy_parallel_only_bound = 6_378_137.0
            * midpoint_latitude.cos()
            * (1.0 - (longitude_delta.to_radians() / 2.0).cos());
        let input_budget = 1.0e-4;
        assert!(legacy_parallel_only_bound <= input_budget);
        assert!(
            sampled_projection_deviation(
                "EPSG:4326",
                &selected.definition,
                XY::new(3.0, 59.9),
                XY::new(3.0, 60.1),
                &state,
            ) > input_budget,
            "the parallel-only model omitted observed meridian deviation",
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn minimum_clearance_uses_original_sites_and_emits_refined_source_support() {
        let west = -0.463_147_336_714_631_5;
        let east = 6.463_147_336_714_631_5;
        let boundary = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_xy(&[XY::new(west, 39.0), XY::new(east, 39.0)]).unwrap(),
            )
            .unwrap(),
        );
        let shape = Shape::geometry_collection(vec![
            boundary,
            Shape::Point(crate::geometry::Point::new_raw_xy(3.0, 39.01)),
        ])
        .unwrap();
        let state = runtime_state_snapshot().unwrap();
        let mut budget = ExpansionBudget::new("minimum_clearance", "input_refinement");

        let witness = geodesic_by_identity(
            &shape,
            Some("EPSG:4326"),
            &state,
            &mut budget,
            LocalInputPurpose::BoundarySupport,
            |sites: &Shape, support: &Shape, refined_chains: &[RefinedChainProvenance]| {
                let Some(witness) =
                    sites.minimum_clearance_plan_with_support(support, refined_chains, false)?
                else {
                    return Ok((
                        Vec::new(),
                        Shape::typed_empty(
                            crate::geometry::GeometryKind::LineString,
                            crate::geometry::CoordinateAxes::XY,
                        ),
                    ));
                };
                let plan = witness.plan.to_vec();
                let points = crate::geometry::emit_from_original(&witness.points, &plan)?;
                let [start, end] = points.as_slice() else {
                    unreachable!()
                };
                Ok((plan, Shape::LineString(LineSeq::segment(*start, *end)?)))
            },
            |source_points, _, plan, _, _| crate::geometry::emit_from_original(source_points, plan),
            |points: Vec<crate::geometry::Point>| {
                if points.is_empty() {
                    Ok(Shape::typed_empty(
                        crate::geometry::GeometryKind::LineString,
                        crate::geometry::CoordinateAxes::XY,
                    ))
                } else {
                    let [start, end] = points.as_slice() else {
                        unreachable!("minimum-clearance witness contains two endpoints");
                    };
                    Ok(Shape::LineString(LineSeq::segment(*start, *end)?))
                }
            },
        )
        .unwrap();

        let Shape::LineString(witness) = witness else {
            panic!("minimum-clearance witness is a line");
        };
        assert_eq!(witness.len(), 2);
        assert_eq!(
            witness.point_at(0),
            crate::geometry::Point::new_raw_xy(3.0, 39.01)
        );
        assert_eq!(witness.point_at(1).y, 39.0);
        assert!((west..=east).contains(&witness.point_at(1).x));
        assert!(budget.used() > 0);
    }
}
