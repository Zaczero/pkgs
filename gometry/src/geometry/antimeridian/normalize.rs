#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::collections::HashMap;

use crate::error::Result;
use crate::geographic::{GeographicError, GeographicFrame, ScalarGeographicView};
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, ExactGeographicTopology, GeographicDerivedPointScan,
    GeometryErrorKind, Point, Polygon, PreparedPlan, RepairMethod, Segment, Shape, ShapeOwnerKey,
    TopologyDomain, ValidationIssue, VisibleXYKey, same_point, shape_encloses_pole_in_frame,
    shape_has_polar_ring_in_frame, shared_segment_part,
};

fn try_topology_split(shape: &Shape, domain: TopologyDomain) -> Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => shape.force_2d(),
        TopologyDomain::Geographic(frame) => shape.force_2d()?.split_antimeridian_in_frame(frame),
    }
}

fn split_in_domain(shape: &Shape, domain: TopologyDomain) -> Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => Ok(shape.clone()),
        TopologyDomain::Geographic(frame) => shape.split_antimeridian_in_frame(frame),
    }
}

fn needs_topology_split(shape: &Shape, domain: TopologyDomain) -> bool {
    match domain {
        TopologyDomain::Cartesian => false,
        TopologyDomain::Geographic(frame) => {
            crate::geometry::crosses_antimeridian_in_frame(shape, frame)
        },
    }
}

fn seam_facts_for_data(
    data: &PreparedPlan,
    frame: GeographicFrame,
) -> crate::geometry::GeographicSeamFacts {
    if frame == GeographicFrame::crs84() {
        data.geographic_seam_facts()
    } else {
        crate::geometry::geographic_seam_facts_in_frame(data.shape(), frame)
    }
}

fn exact_source_seam_contact(
    shape: &Shape,
    frame: GeographicFrame,
    possible_contact: bool,
) -> Result<Option<ExactSourceSeamContact>> {
    if !possible_contact {
        return Ok(None);
    }
    let Ok(view) = ScalarGeographicView::try_new(frame, shape) else {
        return Ok(None);
    };
    let topology =
        ExactGeographicTopology::from_views_with_seam_cut_edge_pairs(frame, std::iter::once(view))?;
    let has_contact = topology.has_source_contact_at_seam()?;
    let cut_edge_pairs = topology.seam_cut_edge_pairs().cloned().ok_or_else(|| {
        GeometryErrorKind::Invalid("exact Q omitted requested seam-cut contacts".into())
    })?;
    Ok(Some(ExactSourceSeamContact {
        has_contact,
        cut_edge_pairs,
    }))
}

#[derive(Clone, Debug)]
struct ExactSourceSeamContact {
    has_contact: bool,
    cut_edge_pairs: crate::geometry::SeamCutEdgePairs,
}

fn exact_topology_failure(error: &crate::error::Error) -> ValidationIssue {
    ValidationIssue::new(
        "topology_normalization_failed",
        format!("exact geographic topology failed: {error}"),
        None,
        "$",
    )
}

#[derive(Debug)]
enum UnwrapChartError {
    Winding,
    Geometry(crate::error::Error),
}

impl From<crate::error::Error> for UnwrapChartError {
    fn from(error: crate::error::Error) -> Self {
        Self::Geometry(error)
    }
}

fn chart_longitude(
    longitude: f64,
    frame: GeographicFrame,
) -> std::result::Result<f64, UnwrapChartError> {
    let axis = frame.longitude();
    let longitude = axis
        .canonicalize(longitude)
        .map_err(|error| {
            UnwrapChartError::Geometry(GeometryErrorKind::antimeridian_split_failed(
                error.to_string(),
            ))
        })?
        .get();
    Ok(if longitude < 0.0 {
        longitude + axis.period()
    } else {
        longitude
    })
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Q admits at most u32 source edges, so the accumulated turn count is exactly representable as f64"
)]
fn longitude_with_turns(longitude: f64, turns: i64, period: f64) -> f64 {
    longitude + turns as f64 * period
}

fn unwrap_coordinates(
    coordinates: &CoordSeq,
    frame: GeographicFrame,
) -> std::result::Result<CoordSeq, UnwrapChartError> {
    let Some(first) = coordinates.first_coord() else {
        return Ok(coordinates.clone());
    };
    let axis = frame.longitude();
    let first_longitude = chart_longitude(first.x, frame)?;
    let mut points = Vec::with_capacity(coordinates.coord_count());
    points.push(first.with_xy(first_longitude, first.y)?);
    let mut previous_source = first;
    let mut previous_longitude = first_longitude;
    let mut turns = 0_i64;
    for point in coordinates.iter_coords().skip(1) {
        let longitude = chart_longitude(point.x, frame)?;
        let step = axis
            .chart_step(previous_source.x, point.x)
            .map_err(|error| {
                UnwrapChartError::Geometry(GeometryErrorKind::antimeridian_split_failed(
                    error.to_string(),
                ))
            })?;
        let delta = step.delta(axis);
        let chart_delta = longitude - previous_longitude;
        let wrap = if delta - chart_delta > axis.half_period() {
            1
        } else if delta - chart_delta < -axis.half_period() {
            -1
        } else {
            0
        };
        turns = turns.checked_add(wrap).ok_or_else(|| {
            UnwrapChartError::Geometry(crate::error::ResourceError::SizeOverflow.into())
        })?;
        let x = longitude_with_turns(longitude, turns, axis.period());
        if !x.is_finite() {
            return Err(UnwrapChartError::Winding);
        }
        points.push(point.with_xy(x, point.y)?);
        previous_source = point;
        previous_longitude = longitude;
    }
    if let Some(last) = coordinates.last_coord()
        && same_point(first, last)
    {
        if turns != 0 {
            return Err(UnwrapChartError::Winding);
        }
        if let Some(last) = points.last_mut() {
            *last = last.with_xy(first_longitude, first.y)?;
        }
    }
    CoordSeq::try_from_points(&points).map_err(UnwrapChartError::from)
}

fn unwrap_contact_chart_inner(
    shape: &Shape,
    frame: GeographicFrame,
) -> std::result::Result<Option<Shape>, UnwrapChartError> {
    match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => Ok(Some(shape.clone())),
        Shape::GeometryCollection(collection) => {
            let mut members = Vec::with_capacity(collection.len());
            for member in collection {
                let Some(member) = unwrap_contact_chart_inner(member, frame)? else {
                    return Ok(None);
                };
                members.push(member);
            }
            Ok(Some(Shape::GeometryCollection(
                collection
                    .try_rebuild_members(members)
                    .map_err(UnwrapChartError::from)?,
            )))
        },
        Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => shape
            .try_map_coordseqs(
                |coordinates| unwrap_coordinates(coordinates, frame),
                |point| Ok(*point),
            )
            .map(Some),
    }
}

fn unwrap_contact_chart(shape: &Shape, frame: GeographicFrame) -> Result<Option<Shape>> {
    match unwrap_contact_chart_inner(shape, frame) {
        Ok(chart) => Ok(chart),
        Err(UnwrapChartError::Winding) => Ok(None),
        Err(UnwrapChartError::Geometry(error)) => Err(error),
    }
}

fn rings_share_source_linework(left: &CoordSeq, right: &CoordSeq) -> bool {
    left.segment_pairs().any(|[left_start, left_end]| {
        let left = Segment {
            start: left_start.xy(),
            end: left_end.xy(),
        };
        right.segment_pairs().any(|[right_start, right_end]| {
            shared_segment_part(left, Segment {
                start: right_start.xy(),
                end: right_end.xy(),
            })
            .is_some()
        })
    })
}

/// Validate relationships involving crossing polygon holes before the public
/// split's set-difference assembly. Difference is deliberately repair-like: an
/// outside hole simply subtracts nothing and overlapping holes collapse to
/// their union. That is useful to `repair`, but validity must diagnose the
/// malformed source rather than bless the normalized result.
fn crossing_hole_relationship_issue(
    shape: &Shape,
    path: &str,
    domain: TopologyDomain,
) -> Result<Option<ValidationIssue>> {
    let TopologyDomain::Geographic(frame) = domain else {
        return Ok(None);
    };
    let polygon_issue = |polygon: &Polygon, path: &str| -> Result<Option<ValidationIssue>> {
        let mut crossing = Vec::new();
        for (index, hole) in polygon.holes.iter().enumerate() {
            let candidate = Shape::Polygon(Polygon::new((*hole).clone(), Vec::new()));
            if crate::geometry::crosses_antimeridian_in_frame(&candidate, frame) {
                crossing.push((index, hole));
            }
        }
        if crossing.is_empty() {
            return Ok(None);
        }

        let crossing_indices: Vec<usize> = crossing.iter().map(|(index, _)| *index).collect();
        let stationary = polygon
            .holes
            .iter()
            .enumerate()
            .filter(|(index, _)| !crossing_indices.contains(index))
            .map(|(_, hole)| hole.clone())
            .collect();
        let container = Shape::Polygon(Polygon::new(polygon.shell.clone(), stationary));
        let container = match try_topology_split(&container, domain) {
            Ok(container) => container,
            Err(error) => {
                if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) {
                    return Err(error);
                }
                return Ok(Some(ValidationIssue::new(
                    "topology_normalization_failed",
                    format!("geographic topology normalization failed: {error}"),
                    None,
                    path,
                )));
            },
        };
        let mut normalized_holes: Vec<(usize, Shape)> = Vec::with_capacity(crossing.len());
        for (index, hole) in crossing {
            let witness = hole.coords().first();
            if rings_share_source_linework(polygon.shell.coords(), hole.coords())
                || polygon
                    .holes
                    .iter()
                    .enumerate()
                    .any(|(other_index, other)| {
                        other_index != index
                            && rings_share_source_linework(other.coords(), hole.coords())
                    })
            {
                return Ok(Some(ValidationIssue::new(
                    "shared_linework",
                    format!("interior ring at index {index} intersects another ring on a line"),
                    witness,
                    path,
                )));
            }
            let hole = Shape::Polygon(Polygon::new(hole.clone(), Vec::new()));
            let hole = match try_topology_split(&hole, domain) {
                Ok(hole) => hole,
                Err(error) => {
                    if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) {
                        return Err(error);
                    }
                    return Ok(Some(ValidationIssue::new(
                        "topology_normalization_failed",
                        format!("geographic topology normalization failed: {error}"),
                        witness,
                        format!("{path}.holes[{index}]"),
                    )));
                },
            };
            if !container.contains(&hole)? {
                return Ok(Some(ValidationIssue::new(
                    "hole_outside_shell",
                    format!(
                        "interior ring at index {index} is not contained within the polygon's exterior"
                    ),
                    witness,
                    path,
                )));
            }
            for (other_index, other) in &normalized_holes {
                let relation = other.relate(&hole)?;
                if relation
                    .as_bytes()
                    .first()
                    .copied()
                    .expect("DE-9IM relations contain nine cells")
                    != b'F'
                {
                    return Ok(Some(ValidationIssue::new(
                        "hole_overlap",
                        format!(
                            "interior ring at index {} and interior ring at index {} intersect on an area",
                            index.min(*other_index),
                            index.max(*other_index),
                        ),
                        witness,
                        path,
                    )));
                }
            }
            normalized_holes.push((index, hole));
        }
        Ok(None)
    };

    match shape {
        Shape::Polygon(polygon) => polygon_issue(polygon, path),
        Shape::MultiPolygon(polygons) => {
            for (index, polygon) in polygons.iter().enumerate() {
                if let Some(issue) = polygon_issue(polygon, &format!("{path}.geometries[{index}]"))?
                {
                    return Ok(Some(issue));
                }
            }
            Ok(None)
        },
        Shape::GeometryCollection(parts) => {
            for (index, part) in parts.iter().enumerate() {
                if let Some(issue) = crossing_hole_relationship_issue(
                    part,
                    &format!("{path}.geometries[{index}]"),
                    domain,
                )? {
                    return Ok(Some(issue));
                }
            }
            Ok(None)
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => Ok(None),
    }
}

fn geographic_admission_validation_issue(
    shape: &Shape,
    frame: GeographicFrame,
) -> Option<ValidationIssue> {
    let diagnostic = ScalarGeographicView::try_new(frame, shape).err()?;
    let member_path = diagnostic
        .location
        .members
        .as_slice()
        .iter()
        .fold("$".to_owned(), |path, member| format!("{path}[{member}]"));
    let path = match diagnostic.location.ring {
        Some(0) => format!("{member_path}.shell"),
        Some(ring) => {
            let hole = ring
                .checked_sub(1)
                .expect("non-shell ring indices follow the shell");
            format!("{member_path}.holes[{hole}]")
        },
        None => member_path,
    };
    let (code, detail) = match diagnostic.source {
        GeographicError::NonFinite { .. } => (
            "non_finite_coordinate",
            "point coordinates must be finite".to_owned(),
        ),
        GeographicError::LatitudeOutOfRange { .. } => {
            ("latitude_out_of_range", diagnostic.source.to_string())
        },
        GeographicError::AmbiguousFullTurnRing { .. } => {
            ("ambiguous_full_turn_ring", diagnostic.source.to_string())
        },
        GeographicError::InvalidAngularFactor { .. }
        | GeographicError::InvalidSweep { .. }
        | GeographicError::ChartTurnOverflow => {
            ("geographic_admission_error", diagnostic.source.to_string())
        },
    };
    Some(ValidationIssue::new(
        code,
        detail,
        Some(Point::new_raw_xy(diagnostic.native.x, diagnostic.native.y)),
        path,
    ))
}

/// Validate stored geometry in its coordinate frame. Projected and CRS-free
/// geometries retain ordinary planar OGC validity.
fn validate_geographic_shape(
    shape: &Shape,
    frame: GeographicFrame,
    facts: crate::geometry::GeographicSeamFacts,
) -> Result<Option<ValidationIssue>> {
    if let Some(issue) = geographic_admission_validation_issue(shape, frame) {
        return Ok(Some(issue));
    }
    validate_geographic_shape_admitted(shape, frame, facts)
}

fn validate_geographic_shape_admitted(
    shape: &Shape,
    frame: GeographicFrame,
    facts: crate::geometry::GeographicSeamFacts,
) -> Result<Option<ValidationIssue>> {
    if facts.possible_contact {
        match exact_source_seam_contact(shape, frame, true) {
            Ok(Some(contact)) if contact.has_contact => {
                if let Some(chart) = unwrap_contact_chart(&shape.force_2d()?, frame)? {
                    return chart.validate_with_seam_cut_edge_pairs(&contact.cut_edge_pairs);
                }
            },
            Ok(Some(_) | None) => {},
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                return Err(error);
            },
            Err(error) => return Ok(Some(exact_topology_failure(&error))),
        }
    }
    if facts.crosses {
        let planar = shape.force_2d()?;
        if let Some(issue) =
            crossing_hole_relationship_issue(&planar, "$", TopologyDomain::Geographic(frame))?
        {
            return Ok(Some(issue));
        }
        try_topology_split(shape, TopologyDomain::Geographic(frame)).map_or_else(
            |error| {
                if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) {
                    return Err(error);
                }
                Ok(Some(ValidationIssue::new(
                    "topology_normalization_failed",
                    format!("geographic topology normalization failed: {error}"),
                    None,
                    "$",
                )))
            },
            |normalized| normalized.validate(),
        )
    } else {
        shape.validate()
    }
}

pub(crate) fn validate_shape_in_frame(
    shape: &Shape,
    domain: TopologyDomain,
) -> Result<Option<ValidationIssue>> {
    match domain {
        TopologyDomain::Cartesian => shape.validate(),
        TopologyDomain::Geographic(frame) => validate_geographic_shape(
            shape,
            frame,
            crate::geometry::geographic_seam_facts_in_frame(shape, frame),
        ),
    }
}

/// Cached-handle twin of [`validate_shape_in_frame`].
pub(crate) fn validate_data_in_frame(
    data: &PreparedPlan,
    domain: TopologyDomain,
) -> Result<Option<ValidationIssue>> {
    match domain {
        TopologyDomain::Cartesian => Ok(data.validate_cached()?.cloned()),
        TopologyDomain::Geographic(frame) => {
            let facts = seam_facts_for_data(data, frame);
            if let Some(issue) = geographic_admission_validation_issue(data.shape(), frame) {
                return Ok(Some(issue));
            }
            if facts.crosses || facts.possible_contact {
                validate_geographic_shape_admitted(data.shape(), frame, facts)
            } else {
                Ok(data.validate_cached()?.cloned())
            }
        },
    }
}

/// Frame-aware simplicity. Seam-contact candidates and seam-crossing inputs
/// answer on normalized topology; ordinary inputs keep the cached verdict.
pub(crate) fn is_simple_data_in_frame(data: &PreparedPlan, domain: TopologyDomain) -> Result<bool> {
    let TopologyDomain::Geographic(frame) = domain else {
        return data.is_simple_cached();
    };
    crate::crs::ensure_geographic_domain_in_frame(frame, data.shape())?;
    let facts = seam_facts_for_data(data, frame);
    if facts.possible_contact {
        match exact_source_seam_contact(data.shape(), frame, true) {
            Ok(Some(contact)) if contact.has_contact => {
                if let Some(chart) = unwrap_contact_chart(&data.shape().force_2d()?, frame)? {
                    return chart.is_simple_with_seam_cut_edge_pairs(&contact.cut_edge_pairs);
                }
            },
            Ok(Some(_) | None) => {},
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                return Err(error);
            },
            Err(_) => return Ok(false),
        }
    }
    if facts.crosses {
        match try_topology_split(data.shape(), domain) {
            Ok(shape) => shape.is_simple(),
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                Err(error)
            },
            Err(_) => Ok(false),
        }
    } else {
        data.is_simple_cached()
    }
}

/// Frame-aware `is_ring`: closure is structural on the original line, while
/// simplicity is evaluated on normalized seam topology.
pub(crate) fn is_ring_data_in_frame(data: &PreparedPlan, domain: TopologyDomain) -> Result<bool> {
    let TopologyDomain::Geographic(frame) = domain else {
        return data.shape().is_ring();
    };
    crate::crs::ensure_geographic_domain_in_frame(frame, data.shape())?;
    if !crate::geometry::shape_is_closed_in_domain(data.shape(), domain) {
        return Ok(false);
    }
    let facts = seam_facts_for_data(data, frame);
    if facts.possible_contact {
        match exact_source_seam_contact(data.shape(), frame, true) {
            Ok(Some(contact)) if contact.has_contact => {
                if let Some(chart) = unwrap_contact_chart(&data.shape().force_2d()?, frame)? {
                    return chart.is_simple_with_seam_cut_edge_pairs(&contact.cut_edge_pairs);
                }
            },
            Ok(Some(_) | None) => {},
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                return Err(error);
            },
            Err(_) => return Ok(false),
        }
    }
    if facts.crosses {
        match try_topology_split(data.shape(), domain) {
            Ok(shape) => shape.is_simple(),
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                Err(error)
            },
            Err(_) => Ok(false),
        }
    } else {
        data.is_simple_cached()
    }
}

/// Frame-aware self-intersection diagnostics, shared by scalar and grouped
/// array output.
pub(crate) fn self_intersections_in_frame(
    shape: &Shape,
    domain: TopologyDomain,
) -> Result<Vec<Point>> {
    let TopologyDomain::Geographic(frame) = domain else {
        return shape.self_intersections();
    };
    crate::crs::ensure_geographic_domain_in_frame(frame, shape)?;
    let facts = crate::geometry::geographic_seam_facts_in_frame(shape, frame);
    if facts.possible_contact {
        match exact_source_seam_contact(shape, frame, true) {
            Ok(Some(contact)) if contact.has_contact => {
                if let Some(chart) = unwrap_contact_chart(shape, frame)? {
                    return chart
                        .self_intersections_with_seam_cut_edge_pairs(&contact.cut_edge_pairs);
                }
            },
            Ok(Some(_) | None) => {},
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                return Err(error);
            },
            Err(_) => return shape.self_intersections(),
        }
    }
    if facts.crosses {
        match try_topology_split(shape, domain) {
            Ok(normalized) => {
                if normalized.is_simple()? {
                    Ok(Vec::new())
                } else {
                    normalized.self_intersections()
                }
            },
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                Err(error)
            },
            Err(_) => shape.self_intersections(),
        }
    } else {
        shape.self_intersections()
    }
}

fn repair_invalid_shape(
    shape: &Shape,
    domain: TopologyDomain,
    method: RepairMethod,
) -> Result<Shape> {
    let repaired = if needs_topology_split(shape, domain) {
        // Preserve source Z/M on ordinary seam crossings. Polar closure with
        // Z/M remains an honest error: no source ordinate exists at a
        // fabricated pole vertex.
        split_in_domain(shape, domain)?.repair(method, false)
    } else {
        shape.repair(method, false)
    }?;
    if let Some(issue) = validate_shape_in_frame(&repaired, domain)? {
        return Err(GeometryErrorKind::repair_failed(format!(
            "repair did not produce valid geometry: {}",
            issue.detail,
        )));
    }
    Ok(repaired)
}

fn reject_ambiguous_ring_repair(issue: &ValidationIssue) -> Result<()> {
    if issue.code == "ambiguous_full_turn_ring" {
        return Err(GeometryErrorKind::repair_failed(format!(
            "an ambiguous full-turn ring has no unique pole closure: {}",
            issue.detail,
        )));
    }
    Ok(())
}

/// Repair only when frame-aware validation finds a defect. `None` means the
/// caller must preserve the original frozen handle/storage unchanged.
pub(crate) fn repair_shape_in_frame(
    shape: &Shape,
    domain: TopologyDomain,
    method: RepairMethod,
) -> Result<Option<Shape>> {
    let Some(issue) = validate_shape_in_frame(shape, domain)? else {
        return Ok(None);
    };
    reject_ambiguous_ring_repair(&issue)?;
    repair_invalid_shape(shape, domain, method).map(Some)
}

/// Cached-handle twin of [`repair_shape_in_frame`].
pub(crate) fn repair_data_in_frame(
    data: &PreparedPlan,
    domain: TopologyDomain,
    method: RepairMethod,
) -> Result<Option<Shape>> {
    let Some(issue) = validate_data_in_frame(data, domain)? else {
        return Ok(None);
    };
    reject_ambiguous_ring_repair(&issue)?;
    repair_invalid_shape(data.shape(), domain, method).map(Some)
}

/// Shift canonical western longitudes by one native full turn so a crossing
/// geometry is contiguous for the planar centroid kernel.
fn unwrap_longitude(shape: &Shape, frame: GeographicFrame) -> Result<Shape> {
    let axis = frame.longitude();
    shape.map_xy_with(|x, y| {
        let longitude = axis
            .canonicalize(x)
            .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?
            .get();
        Ok((
            if longitude < 0.0 {
                longitude + axis.period()
            } else {
                longitude
            },
            y,
        ))
    })
}

/// Canonicalize a native longitude into the frame's half-open seam interval.
fn wrap_longitude(lon: f64, frame: GeographicFrame) -> Result<f64> {
    frame
        .longitude()
        .canonicalize(lon)
        .map(|longitude| longitude.get())
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

/// Rewrap a derived point's longitude after an unwrapped planar kernel.
fn rewrap_point(point: Point, frame: GeographicFrame) -> Result<Point> {
    point.with_xy(wrap_longitude(point.x, frame)?, point.y)
}

/// Derived representative point (centroid / `point_on_surface`) on a crossing
/// or seam-connected geographic geometry. Two strategies, because
/// unwrap-rewrap cannot serve both:
///
/// * `interior` callers (`point_on_surface`, which must return a point strictly
///   inside) AND any pole-enclosing geometry route through the seam+pole SPLIT —
///   the unwrapped form of a polar cap is a degenerate longitude sliver (a
///   longitude shift can't reconstruct the cap), and the split's planar kernel
///   yields a point genuinely inside the true region. The split is in the
///   frame's canonical longitude interval, so no rewrap is needed.
///   `force_2d` keeps the split infallible
///   (a Z/M pole closure would otherwise trip the ordinate gate).
/// * every other crossing `centroid` uses unwrap-rewrap for the planar centroid
///   on the contiguous native longitude chart (the split's two opposite-seam
///   halves would average to a meaningless mid-longitude).
#[derive(Clone, Copy)]
pub(crate) enum DerivedPointStrategy {
    Centroid,
    Interior,
}

fn derived_point_unwrapped(
    shape: &Shape,
    frame: GeographicFrame,
    strategy: DerivedPointStrategy,
    kernel: impl FnOnce(&Shape) -> Result<Shape>,
) -> Result<Shape> {
    let encloses_pole = shape_encloses_pole_in_frame(shape, frame, true)
        || shape_encloses_pole_in_frame(shape, frame, false);
    if matches!(strategy, DerivedPointStrategy::Interior) || encloses_pole {
        return kernel(&try_topology_split(
            shape,
            TopologyDomain::Geographic(frame),
        )?);
    }
    let unwrapped = unwrap_longitude(shape, frame)?;
    match kernel(&unwrapped)? {
        Shape::Point(point) => Ok(Shape::Point(rewrap_point(point, frame)?)),
        other @ (Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..)) => Ok(other),
    }
}

fn derived_point_needs_unwrap(shape: &Shape, frame: GeographicFrame) -> bool {
    // Split seam-side pieces are multipart; preserve the crossing-only route
    // for simple shapes and compare seam contacts across multipart chains.
    if let Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) = shape {
        return false;
    }
    if let Shape::LineString(chain) = shape {
        return crate::geometry::chain_crosses_antimeridian(chain, frame);
    }
    if let Shape::Polygon(_) = shape {
        return crate::geometry::crosses_antimeridian_in_frame(shape, frame);
    }

    let mut scan = GeographicDerivedPointScan::new(frame);
    let crosses = matches!(
        shape.for_each_first_xy_segment_chain(|chain| {
            if scan.scan_chain(chain) {
                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        }),
        std::ops::ControlFlow::Break(())
    );
    crosses || scan.connects_across_seam()
}

/// Shared scalar/array gate for derived geographic points.
pub(crate) fn unary_antimeridian_derived(
    shape: &Shape,
    domain: TopologyDomain,
    strategy: DerivedPointStrategy,
    kernel: impl FnOnce(&Shape) -> Result<Shape>,
) -> Result<Shape> {
    match domain {
        TopologyDomain::Geographic(frame) if derived_point_needs_unwrap(shape, frame) => {
            derived_point_unwrapped(shape, frame, strategy, kernel)
        },
        TopologyDomain::Cartesian | TopologyDomain::Geographic(_) => kernel(shape),
    }
}

/// Geographic-crossing bounds in the west>east convention: ``minx`` is the
/// minimum eastern (``lon >= 0``) longitude and ``maxx`` the maximum western
/// (``lon < 0``) longitude; latitude extrema are ordinary planar min/max.
pub(crate) fn geographic_crossing_bounds(shape: &Shape) -> Option<Bounds> {
    geographic_crossing_bounds_in_frame(shape, GeographicFrame::crs84())
}

pub(crate) fn geographic_crossing_bounds_in_frame(
    shape: &Shape,
    frame: GeographicFrame,
) -> Option<Bounds> {
    geographic_crossing_bounds_for_shapes_in_frame(std::iter::once(shape), frame)
}

pub(crate) fn geographic_crossing_bounds_for_shapes_in_frame(
    shapes: impl IntoIterator<Item = impl std::borrow::Borrow<Shape>>,
    frame: GeographicFrame,
) -> Option<Bounds> {
    let axis = frame.longitude();
    let mut point_memo = CrossingBoundsMemo::default();
    let mut summary = CrossingBoundsSummary::default();
    let mut encloses_north = false;
    let mut encloses_south = false;
    let mut has_polar_ring = false;
    for shape in shapes {
        let shape = shape.borrow();
        summary.merge(point_memo.shape_summary(shape, frame));
        encloses_north |= shape_encloses_pole_in_frame(shape, frame, true);
        encloses_south |= shape_encloses_pole_in_frame(shape, frame, false);
        has_polar_ring |= shape_has_polar_ring_in_frame(shape, frame);
    }
    debug_assert!(
        (summary.eastern_min.is_some() || summary.western_max.is_some()) == summary.miny.is_some()
            && summary.miny.is_some() == summary.maxy.is_some(),
        "each summarized coordinate contributes longitude and both latitude extrema"
    );
    let seam = axis
        .canonicalize(axis.half_period())
        .expect("a longitude axis half-period is its finite seam")
        .get();
    let (mut minx, mut miny, mut maxx, mut maxy) = (
        summary.eastern_min.unwrap_or(seam),
        summary.miny?,
        summary.western_max.unwrap_or(seam),
        summary.maxy?,
    );
    // A pole-enclosing ring covers the whole polar cap, so its envelope reaches
    // the pole and spans every longitude — the eastern_min/western_max sliver
    // (which only describes the seam window) would otherwise drop the cap. Mirrors
    // the spatial index's geographic envelopes so public bounds() and the index
    // agree.
    if has_polar_ring {
        (minx, maxx) = (-axis.half_period(), axis.half_period());
    }
    if encloses_north {
        maxy = frame.pole();
    }
    if encloses_south {
        miny = -frame.pole();
    }
    Some(Bounds::new_raw(minx, miny, maxx, maxy))
}

#[derive(Clone, Copy, Default)]
struct CrossingBoundsSummary {
    eastern_min: Option<f64>,
    western_max: Option<f64>,
    miny: Option<f64>,
    maxy: Option<f64>,
}

impl CrossingBoundsSummary {
    fn include_point(&mut self, point: Point, frame: GeographicFrame) {
        let longitude = frame
            .longitude()
            .canonicalize(point.x)
            .map_or(point.x, |value| value.get());
        if longitude >= 0.0 {
            include_min(&mut self.eastern_min, longitude);
        } else {
            include_max(&mut self.western_max, longitude);
        }
        include_min(&mut self.miny, point.y);
        include_max(&mut self.maxy, point.y);
    }

    fn merge(&mut self, other: Self) {
        if let Some(value) = other.eastern_min {
            include_min(&mut self.eastern_min, value);
        }
        if let Some(value) = other.western_max {
            include_max(&mut self.western_max, value);
        }
        if let Some(value) = other.miny {
            include_min(&mut self.miny, value);
        }
        if let Some(value) = other.maxy {
            include_max(&mut self.maxy, value);
        }
    }
}

fn include_min(accumulator: &mut Option<f64>, value: f64) {
    *accumulator = Some(accumulator.map_or(value, |current| current.min(value)));
}

fn include_max(accumulator: &mut Option<f64>, value: f64) {
    *accumulator = Some(accumulator.map_or(value, |current| current.max(value)));
}

#[derive(Default)]
struct CrossingBoundsMemo {
    collections: HashMap<ShapeOwnerKey, CrossingBoundsSummary>,
    xy_runs: HashMap<VisibleXYKey, CrossingBoundsSummary>,
}

impl CrossingBoundsMemo {
    fn coord_summary(
        &mut self,
        coords: &CoordSeq,
        frame: GeographicFrame,
    ) -> CrossingBoundsSummary {
        if let Some(key) = coords.shared_xy_key()
            && let Some(&summary) = self.xy_runs.get(&key)
        {
            return summary;
        }
        let summary = coords
            .iter()
            .fold(CrossingBoundsSummary::default(), |mut acc, point| {
                acc.include_point(point, frame);
                acc
            });
        if let Some(key) = coords.shared_xy_key() {
            self.xy_runs.insert(key, summary);
        }
        summary
    }

    fn polygon_summary(
        &mut self,
        polygon: &Polygon,
        frame: GeographicFrame,
    ) -> CrossingBoundsSummary {
        let mut summary = CrossingBoundsSummary::default();
        for ring in polygon.rings() {
            summary.merge(self.coord_summary(ring.coords(), frame));
        }
        summary
    }

    fn shape_summary(&mut self, shape: &Shape, frame: GeographicFrame) -> CrossingBoundsSummary {
        if let Shape::GeometryCollection(collection) = shape {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            if let Some(&summary) = owner.as_ref().and_then(|owner| self.collections.get(owner)) {
                return summary;
            }
            let mut summary = CrossingBoundsSummary::default();
            for member in collection {
                summary.merge(self.shape_summary(member, frame));
            }
            if let Some(owner) = owner {
                self.collections.insert(owner, summary);
            }
            return summary;
        }
        match shape {
            Shape::Point(point) => {
                let mut summary = CrossingBoundsSummary::default();
                summary.include_point(*point, frame);
                summary
            },
            Shape::MultiPoint(coords) => self.coord_summary(coords, frame),
            Shape::LineString(coords) => self.coord_summary(coords, frame),
            Shape::MultiLineString(lines) => {
                let mut summary = CrossingBoundsSummary::default();
                for line in lines {
                    summary.merge(self.coord_summary(line.as_coords(), frame));
                }
                summary
            },
            Shape::Polygon(polygon) => self.polygon_summary(polygon, frame),
            Shape::MultiPolygon(polygons) => {
                let mut summary = CrossingBoundsSummary::default();
                for polygon in polygons {
                    summary.merge(self.polygon_summary(polygon, frame));
                }
                summary
            },
            Shape::GeometryCollection(_) => unreachable!("handled above"),
            Shape::Empty(..) => CrossingBoundsSummary::default(),
        }
    }
}
