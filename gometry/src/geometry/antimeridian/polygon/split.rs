use super::assemble::{assemble_polygons, extend_over_poles, polygons_from_shape};
use super::*;

/// A pole-closure cap fabricates ±90 vertices with no source segment, so a
/// measured/3D input cannot honestly keep its ordinates there. Reject Z/M only
/// when such a cap vertex ACTUALLY survived into the assembled output — a plain
/// seam crossing (no pole) fabricates none and its seam vertices interpolate
/// fine, so it must not be rejected. (The check is post-assembly because the
/// split speculatively explores both pole closures before discarding the cap.)
#[expect(
    clippy::float_cmp,
    reason = "cap vertices are the exact literals ±90; any other latitude is real data"
)]
fn reject_fabricated_pole_ordinates(source: &Shape, polygons: &[Polygon]) -> Result<()> {
    if source.axes() == CoordinateAxes::XY {
        return Ok(());
    }
    let fabricated_pole_vertex = polygons.iter().any(|polygon| {
        std::iter::once(polygon.shell.coords())
            .chain(polygon.holes.iter().map(Ring::coords))
            .any(|ring| {
                ring.iter_coords()
                    .any(|coord| coord.y == 90.0 || coord.y == -90.0)
            })
    });
    if fabricated_pole_vertex {
        Err(GeometryErrorKind::AntimeridianPoleOrdinates.into())
    } else {
        Ok(())
    }
}

pub(super) fn ring_is_ccw(coords: &[Point]) -> bool {
    if coords.len() >= 2
        && coords
            .first()
            .zip(coords.last())
            .is_some_and(|(first, last)| same_point(*first, *last))
    {
        ring_winding(coords).is_ccw()
    } else {
        open_point_cycle_winding(coords).is_ccw()
    }
}

/// Split one polygon into its seam-following parts with silent winding fixes.
/// `preferred` forces the pole-closure direction (the caller knows which pole
/// the ring encircles); `Auto` infers it and retries the other direction only
/// if the inference is invalid. Z/M is rejected only when a pole cap actually
/// survives into the output (see [`reject_fabricated_pole_ordinates`]).
pub(super) fn split_polygon<M: ChartMetric>(
    source: &Shape,
    polygon: &Polygon,
    preferred: PoleClosure,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Vec<Polygon>> {
    let preferred = match preferred {
        PoleClosure::Auto => pole_closure_for_ring(&polygon.shell),
        closure @ PoleClosure::Chart(_) => closure,
    };
    let polygons = split_polygon_inner(polygon, preferred, metric_access)?;
    reject_fabricated_pole_ordinates(source, &polygons)?;
    Ok(polygons)
}

fn split_polygon_inner<M: ChartMetric>(
    polygon: &Polygon,
    preferred: PoleClosure,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Vec<Polygon>> {
    split_polygon_inner_in_mode(polygon, preferred, HoleOverlayMode::Select, metric_access)
}

fn split_polygon_inner_in_mode<M: ChartMetric>(
    polygon: &Polygon,
    preferred: PoleClosure,
    mut overlay_mode: HoleOverlayMode,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Vec<Polygon>> {
    // Split crossing holes as independent areal operands, then subtract them
    // from the split shell.  Treating shell and hole seam fragments as one
    // undifferentiated stitching pool works for ordinary crossings, but it
    // cannot represent nested polar caps: both rings independently require a
    // pole closure, and the pooled closest-start walk joins them into invalid
    // self-intersecting rings. Set difference is the structural operation the
    // source polygon already expresses. Already-cut chart parts use Cartesian
    // coordinates, including zero-turn world bands with their interior
    // longitude witnesses. A nonzero longitude turn remains quotient support
    // and needs Q; its result is cut back into chart pieces before publication.
    // A hole tracing the seam is split with the crossing holes: the polygon's
    // interior lies across the seam from it.
    let mut crossing_holes = Vec::new();
    let mut stationary_holes = Vec::new();
    for hole in polygon.holes.iter() {
        let points = collect_points(hole.coords());
        match split_ring_at_seam_with_metric(&points, GeographicFrame::crs84(), metric_access)? {
            SeamRing::Whole(spelled) if !ring_follows_seam(&points) => {
                stationary_holes.push(Ring::try_from_closed(CoordSeq::try_from_points(&spelled)?)?);
            },
            SeamRing::Pieces(_) | SeamRing::Whole(_) => crossing_holes.push(hole.clone()),
        }
    }
    if !crossing_holes.is_empty() {
        let axes = polygon.shell.coords().axes();
        let shell_only = Polygon::new(polygon.shell.clone(), stationary_holes);
        let shell_parts =
            split_polygon_inner_in_mode(&shell_only, preferred, overlay_mode, metric_access)?;
        let mut result = Shape::narrow_polygons(axes, shell_parts);
        for hole in crossing_holes {
            let hole_axes = hole.coords().axes();
            let hole_closure = pole_closure_for_ring(&hole);
            let hole_parts = split_polygon_inner_in_mode(
                &Polygon::new(hole, Vec::new()),
                hole_closure,
                overlay_mode,
                metric_access,
            )?;
            let hole_shape = Shape::narrow_polygons(hole_axes, hole_parts);
            let frame = GeographicFrame::crs84();
            let operands = [&result, &hole_shape];
            let use_quotient = match overlay_mode {
                HoleOverlayMode::Select => {
                    shape_has_longitude_turn_in_frame(&result, frame)?
                        || shape_has_longitude_turn_in_frame(&hole_shape, frame)?
                },
                HoleOverlayMode::Quotient => true,
                HoleOverlayMode::Chart => false,
            };
            result = if use_quotient {
                overlay_mode = HoleOverlayMode::Quotient;
                match metric_access {
                    Some(metric_access) => crate::geometry::overlay_all_with_metric(
                        &operands,
                        TopologyDomain::Geographic(frame),
                        crate::geometry::OverlayOp::Difference,
                        Strictness::Strict,
                        metric_access,
                    )?,
                    None => crate::geometry::overlay_all(
                        &operands,
                        TopologyDomain::Geographic(frame),
                        crate::geometry::OverlayOp::Difference,
                        Strictness::Strict,
                    )?,
                }
            } else {
                crate::geometry::overlay_all_geographic_chart(
                    &operands,
                    frame,
                    crate::geometry::OverlayOp::Difference,
                    Strictness::Strict,
                    metric_access,
                )?
            };
        }
        let polygons = polygons_from_shape(result)?;
        if overlay_mode != HoleOverlayMode::Quotient {
            return Ok(polygons);
        }
        // Q selects the correct quotient set, but this API publishes cut-chart geometry.
        let mut charted = Vec::new();
        for polygon in polygons {
            let closure = pole_closure_for_ring(&polygon.shell);
            charted.extend(split_polygon_inner_in_mode(
                &polygon,
                closure,
                HoleOverlayMode::Chart,
                metric_access,
            )?);
        }
        return Ok(charted);
    }

    let segments = match split_ring_at_seam_with_metric(
        &collect_points(polygon.shell.coords()),
        GeographicFrame::crs84(),
        metric_access,
    )? {
        SeamRing::Whole(shell) => {
            let shell = Ring::try_from_closed(CoordSeq::try_from_points(&shell)?)?;
            return Ok(vec![Polygon::new(shell, stationary_holes)]);
        },
        SeamRing::Pieces(pieces) => pieces,
    };
    let kept_holes: Vec<&Ring> = stationary_holes.iter().collect();
    if preferred != PoleClosure::Auto {
        // Caller knows the pole: close over it directly, no inference/retry.
        let (stitched, _) = extend_over_poles(segments, preferred)?;
        return assemble_polygons(stitched, &kept_holes);
    }
    let (stitched, capped) = extend_over_poles(segments.clone(), PoleClosure::Auto)?;
    let polygons = assemble_polygons(stitched, &kept_holes)?;
    if !capped
        || polygons.iter().try_fold(true, |ok, polygon| {
            Ok::<_, crate::error::Error>(ok && polygon_is_valid(polygon)?)
        })?
    {
        return Ok(polygons);
    }
    // The automatic closure picked a pole that self-intersects the ring.
    // Try each direction; a unique valid candidate wins, a tie is genuinely
    // ambiguous, and when nothing is valid the input itself was broken. Never
    // return the known-invalid automatic candidate as a plausible geometry.
    let mut valid = Vec::new();
    for closure in [
        PoleClosure::Chart(ChartPole::North),
        PoleClosure::Chart(ChartPole::South),
    ] {
        let (forced, _) = extend_over_poles(segments.clone(), closure)?;
        if let Ok(candidate) = assemble_polygons(forced, &kept_holes)
            && candidate.iter().try_fold(true, |ok, polygon| {
                Ok::<_, crate::error::Error>(ok && polygon_is_valid(polygon)?)
            })?
        {
            valid.push(candidate);
        }
    }
    match <[Vec<Polygon>; 1]>::try_from(valid) {
        Ok([winner]) => Ok(winner),
        Err(candidates) if candidates.is_empty() => {
            Err(GeometryErrorKind::antimeridian_split_failed(
                "neither pole closure produces valid polygon rings",
            ))
        },
        Err(_) => Err(GeometryErrorKind::antimeridian_split_failed(
            "pole closure is ambiguous: both directions produce valid rings",
        )),
    }
}

fn shape_has_longitude_turn_in_frame(shape: &Shape, frame: GeographicFrame) -> Result<bool> {
    fn ring_has_longitude_turn(ring: &Ring, frame: GeographicFrame) -> Result<bool> {
        let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
            .map_err(|error| split_view_error(&error))?;
        Ok(summary.chart_turn != 0)
    }

    fn polygon_has_longitude_turn(polygon: &Polygon, frame: GeographicFrame) -> Result<bool> {
        if ring_has_longitude_turn(&polygon.shell, frame)? {
            return Ok(true);
        }
        for hole in polygon.holes.iter() {
            if ring_has_longitude_turn(hole, frame)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    match shape {
        Shape::Polygon(polygon) => polygon_has_longitude_turn(polygon, frame),
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                if polygon_has_longitude_turn(polygon, frame)? {
                    return Ok(true);
                }
            }
            Ok(false)
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                if shape_has_longitude_turn_in_frame(geometry, frame)? {
                    return Ok(true);
                }
            }
            Ok(false)
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => Ok(false),
    }
}

/// Resolve pole ownership from the admitted source ring before chart stitching.
fn pole_closure_for_ring(ring: &Ring) -> PoleClosure {
    match (
        ring_encloses_pole(ring, ChartPole::North),
        ring_encloses_pole(ring, ChartPole::South),
    ) {
        (true, false) => PoleClosure::Chart(ChartPole::North),
        (false, true) => PoleClosure::Chart(ChartPole::South),
        _ => PoleClosure::Auto,
    }
}
