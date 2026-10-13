use crate::error::Result;
use crate::geographic::chart::{point_set_centroid, point_set_surface};
use crate::geographic::lonlat::geographic_admission_error;
use crate::geographic::{
    ChartPole, GeographicError, GeographicFrame, GeographicSupportEnvelope, Latitude,
    ScalarGeographicView, SeamIdentity,
};
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, ExactGeographicTopology, GeographicDerivedPointScan,
    GeometryErrorKind, Point, Polygon, PreparedPlan, RepairMethod, Shape, SourceArcContext,
    TopologyDomain, ValidationIssue, same_point, shape_encloses_pole_in_frame,
};

fn try_topology_split(shape: &Shape, domain: TopologyDomain) -> Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => shape.force_2d(),
        TopologyDomain::Geographic(frame) => shape.force_2d()?.split_antimeridian_in_frame(frame),
    }
}

fn split_in_domain(
    shape: &Shape,
    domain: TopologyDomain,
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => Ok(shape.clone()),
        TopologyDomain::Geographic(frame) => {
            shape.split_antimeridian_in_frame_with_metric(frame, context.metric_access())
        },
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

fn validate_polygon_structure(
    polygon: &Polygon,
    name: &str,
    path: &str,
) -> Option<ValidationIssue> {
    if let Some(issue) = crate::geometry::validate_ring(&polygon.shell, || {
        (format!("{name} shell"), format!("{path}.shell"))
    }) {
        return Some(issue);
    }
    polygon.holes.iter().enumerate().find_map(|(index, hole)| {
        crate::geometry::validate_ring(hole, || {
            (format!("{name} hole"), format!("{path}.holes[{index}]"))
        })
    })
}

pub(in crate::geometry) fn validate_structure(
    shape: &Shape,
    path: &str,
) -> Option<ValidationIssue> {
    match shape {
        Shape::Point(point) => crate::geometry::validate_point(*point, path),
        Shape::MultiPoint(points) => crate::geometry::validate_points(points, path),
        Shape::LineString(line) => crate::geometry::validate_line(line, "line string")
            .map(|issue| issue.with_path_prefix(path)),
        Shape::MultiLineString(lines) => lines.iter().enumerate().find_map(|(index, line)| {
            crate::geometry::validate_line(line, "multi line string")
                .map(|issue| issue.with_path_prefix(&format!("{path}[{index}]")))
        }),
        Shape::Polygon(polygon) => validate_polygon_structure(polygon, "polygon", path),
        Shape::MultiPolygon(polygons) => {
            polygons.iter().enumerate().find_map(|(index, polygon)| {
                validate_polygon_structure(
                    polygon,
                    "multi polygon member",
                    &format!("{path}[{index}]"),
                )
            })
        },
        Shape::GeometryCollection(collection) => collection
            .iter()
            .enumerate()
            .find_map(|(index, member)| validate_structure(member, &format!("{path}[{index}]"))),
        Shape::Empty(..) => None,
    }
}

#[derive(Debug)]
pub(crate) enum UnwrapChartError {
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

pub(crate) fn unwrap_coordinates(
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

pub(crate) fn unwrap_contact_chart(shape: &Shape, frame: GeographicFrame) -> Result<Option<Shape>> {
    match unwrap_contact_chart_inner(shape, frame) {
        Ok(chart) => Ok(chart),
        Err(UnwrapChartError::Winding) => Ok(None),
        Err(UnwrapChartError::Geometry(error)) => Err(error),
    }
}

fn first_latitude_issue(shape: &Shape, frame: GeographicFrame) -> Result<Option<ValidationIssue>> {
    fn consider_point(
        point: Point,
        path: &str,
        frame: GeographicFrame,
        best: &mut Option<(Point, String)>,
    ) {
        let Err(GeographicError::LatitudeOutOfRange { .. }) = Latitude::try_new(frame, point.y)
        else {
            return;
        };
        let replace = best.as_ref().is_none_or(|(known, known_path)| {
            point
                .y
                .total_cmp(&known.y)
                .then_with(|| point.x.to_bits().cmp(&known.x.to_bits()))
                .then_with(|| path.cmp(known_path))
                .is_lt()
        });
        if replace {
            *best = Some((point, path.to_owned()));
        }
    }

    fn consider<C: crate::geometry::Coordinates + ?Sized>(
        coordinates: &C,
        path: &str,
        frame: GeographicFrame,
        best: &mut Option<(Point, String)>,
    ) {
        for point in coordinates.iter_coords() {
            consider_point(point, path, frame, best);
        }
    }

    fn visit(
        shape: &Shape,
        path: &str,
        frame: GeographicFrame,
        best: &mut Option<(Point, String)>,
    ) -> Result<()> {
        match shape {
            Shape::Point(point) => {
                consider_point(*point, path, frame, best);
                Ok(())
            },
            Shape::MultiPoint(points) => {
                consider(points, path, frame, best);
                Ok(())
            },
            Shape::LineString(line) => {
                consider(line, path, frame, best);
                Ok(())
            },
            Shape::MultiLineString(lines) => {
                for (index, line) in lines.iter().enumerate() {
                    consider(line, &format!("{path}[{index}]"), frame, best);
                }
                Ok(())
            },
            Shape::Polygon(polygon) => {
                consider(&polygon.shell, &format!("{path}.shell"), frame, best);
                for (index, hole) in polygon.holes.iter().enumerate() {
                    consider(hole, &format!("{path}.holes[{index}]"), frame, best);
                }
                Ok(())
            },
            Shape::MultiPolygon(polygons) => {
                for (index, polygon) in polygons.iter().enumerate() {
                    let polygon_path = format!("{path}[{index}]");
                    consider(
                        &polygon.shell,
                        &format!("{polygon_path}.shell"),
                        frame,
                        best,
                    );
                    for (hole_index, hole) in polygon.holes.iter().enumerate() {
                        consider(
                            hole,
                            &format!("{polygon_path}.holes[{hole_index}]"),
                            frame,
                            best,
                        );
                    }
                }
                Ok(())
            },
            Shape::GeometryCollection(collection) => {
                for (index, member) in collection.iter().enumerate() {
                    visit(member, &format!("{path}[{index}]"), frame, best)?;
                }
                Ok(())
            },
            Shape::Empty(..) => Ok(()),
        }
    }

    let mut best = None;
    visit(shape, "$", frame, &mut best)?;
    Ok(best.map(|(point, path)| {
        ValidationIssue::new(
            "latitude_out_of_range",
            GeographicError::LatitudeOutOfRange { value: point.y }.to_string(),
            Some(point),
            path,
        )
    }))
}

fn ring_coordinates<'a>(shape: &'a Shape, members: &[usize], ring: usize) -> Option<&'a CoordSeq> {
    let Some((&member, rest)) = members.split_first() else {
        let Shape::Polygon(polygon) = shape else {
            return None;
        };
        return if ring == 0 {
            Some(polygon.shell.coords())
        } else {
            polygon
                .holes
                .get(ring.checked_sub(1)?)
                .map(crate::geometry::Ring::coords)
        };
    };
    match shape {
        Shape::GeometryCollection(collection) => {
            ring_coordinates(collection.as_slice().get(member)?, rest, ring)
        },
        Shape::MultiPolygon(polygons) if rest.is_empty() => {
            let polygon = polygons.get(member)?;
            if ring == 0 {
                Some(polygon.shell.coords())
            } else {
                polygon
                    .holes
                    .get(ring.checked_sub(1)?)
                    .map(crate::geometry::Ring::coords)
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::Empty(..) => None,
    }
}

fn ambiguous_ring_witness(
    shape: &Shape,
    diagnostic: &crate::geographic::GeographicDiagnostic,
    frame: GeographicFrame,
) -> Result<Point> {
    let ring = diagnostic.location.ring.ok_or_else(|| {
        GeometryErrorKind::Invalid("ambiguous geographic admission has no source ring".into())
    })?;
    let coords =
        ring_coordinates(shape, diagnostic.location.members.as_slice(), ring).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "ambiguous geographic admission has no source coordinates".into(),
            )
        })?;
    let axis = frame.longitude();
    let mut best: Option<Point> = None;
    for point in coords.iter_coords() {
        let longitude = axis
            .canonicalize(point.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let x = if Latitude::try_new(frame, point.y)
            .is_ok_and(|latitude| latitude.identity(frame) == SeamIdentity::Pole)
        {
            0.0
        } else {
            longitude.get()
        };
        let candidate = Point::new_raw_xy(x, point.y);
        let replace = best.is_none_or(|known| {
            candidate
                .y
                .total_cmp(&known.y)
                .then_with(|| candidate.x.total_cmp(&known.x))
                .is_lt()
        });
        if replace {
            best = Some(candidate);
        }
    }
    best.ok_or_else(|| GeometryErrorKind::Invalid("ambiguous source ring is empty".into()).into())
}

fn geographic_admission_validation_issue(
    shape: &Shape,
    frame: GeographicFrame,
    diagnostic: &crate::geographic::GeographicViewError,
) -> Result<Option<ValidationIssue>> {
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
    let issue = match &diagnostic.source {
        GeographicError::NonFinite { .. } => ValidationIssue::new(
            "non_finite_coordinate",
            "point coordinates must be finite",
            Some(Point::new_raw_xy(diagnostic.native.x, diagnostic.native.y)),
            path,
        ),
        GeographicError::LatitudeOutOfRange { .. } => {
            return first_latitude_issue(shape, frame)?.map_or_else(
                || {
                    Err(GeometryErrorKind::Invalid(
                        "geographic admission found no out-of-range latitude".into(),
                    )
                    .into())
                },
                |issue| Ok(Some(issue)),
            );
        },
        GeographicError::AmbiguousFullTurnRing { .. } => ValidationIssue::new(
            "ambiguous_full_turn_ring",
            diagnostic.source.to_string(),
            Some(ambiguous_ring_witness(shape, diagnostic, frame)?),
            path,
        ),
        GeographicError::ChartTurnOverflow => {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        },
        GeographicError::InvalidAngularFactor { .. } => {
            return Err(geographic_admission_error(diagnostic));
        },
    };
    Ok(Some(issue))
}

/// Validate stored geometry in its coordinate frame.
///
/// A geographic input whose stored longitude/latitude coordinates form one
/// faithful chart keeps the cached planar validity rules. Other geographic
/// inputs are evaluated from exact quotient incidence in Q; validity never
/// depends on shifting source longitudes into a rounded contact chart.
fn validate_geographic_shape(
    shape: &Shape,
    frame: GeographicFrame,
) -> Result<Option<ValidationIssue>> {
    if let Some(issue) = validate_structure(shape, "$") {
        return Ok(Some(issue));
    }
    let view = match geographic_validation_view(shape, frame)? {
        GeographicValidationView::Admitted(view) => view,
        GeographicValidationView::Issue(issue) => return Ok(Some(issue)),
    };
    validate_admitted_shape(shape, frame, view, "$")
}

enum GeographicValidationView<'a> {
    Admitted(ScalarGeographicView<'a>),
    Issue(ValidationIssue),
}

fn geographic_validation_view(
    shape: &Shape,
    frame: GeographicFrame,
) -> Result<GeographicValidationView<'_>> {
    match ScalarGeographicView::try_new(frame, shape) {
        Ok(view) => Ok(GeographicValidationView::Admitted(view)),
        Err(diagnostic) => geographic_admission_validation_issue(shape, frame, &diagnostic)?
            .map_or_else(
                || {
                    Err(GeometryErrorKind::Invalid(
                        "geographic admission failed without a validation issue".into(),
                    )
                    .into())
                },
                |issue| Ok(GeographicValidationView::Issue(issue)),
            ),
    }
}

fn validate_admitted_shape(
    shape: &Shape,
    frame: GeographicFrame,
    view: ScalarGeographicView<'_>,
    path: &str,
) -> Result<Option<ValidationIssue>> {
    if let Shape::GeometryCollection(collection) = shape {
        for (index, member) in collection.iter().enumerate() {
            if let Some(issue) = validate_admitted_shape(
                member,
                frame,
                ScalarGeographicView::previously_admitted(frame, member),
                &format!("{path}[{index}]"),
            )? {
                return Ok(Some(issue));
            }
        }
        return Ok(None);
    }

    let route = crate::geometry::classify_geographic_validity_chart(view);
    validate_routed_shape(shape, frame, view, route, path)
}

fn validate_routed_shape(
    shape: &Shape,
    frame: GeographicFrame,
    view: ScalarGeographicView<'_>,
    route: crate::geometry::GeographicTopologyRoute,
    path: &str,
) -> Result<Option<ValidationIssue>> {
    let issue = match route {
        crate::geometry::GeographicTopologyRoute::ExactPartition => shape.validate()?,
        crate::geometry::GeographicTopologyRoute::Quotient => {
            ExactGeographicTopology::from_views(frame, std::iter::once(view))?
                .validity_issue(shape)?
        },
    };
    Ok(issue.map(|issue| issue.with_path_prefix(path)))
}

fn validate_routed_data(
    data: &PreparedPlan,
    view: ScalarGeographicView<'_>,
    route: crate::geometry::GeographicTopologyRoute,
    path: &str,
) -> Result<Option<ValidationIssue>> {
    let issue = match route {
        crate::geometry::GeographicTopologyRoute::ExactPartition => {
            data.validate_cached()?.cloned()
        },
        crate::geometry::GeographicTopologyRoute::Quotient => data.quotient_validity(view)?,
    };
    Ok(issue.map(|issue| issue.with_path_prefix(path)))
}

pub(crate) fn validate_shape_in_frame(
    shape: &Shape,
    domain: TopologyDomain,
) -> Result<Option<ValidationIssue>> {
    match domain {
        TopologyDomain::Cartesian => shape.validate(),
        TopologyDomain::Geographic(frame) => validate_geographic_shape(shape, frame),
    }
}

/// Cached-handle twin of [`validate_shape_in_frame`].
///
/// The packed ordinary-coordinate certificate remains the array fast path;
/// rows needing geographic admission or quotient topology use this lane.
pub(crate) fn validate_data_in_frame(
    data: &PreparedPlan,
    domain: TopologyDomain,
) -> Result<Option<ValidationIssue>> {
    match domain {
        TopologyDomain::Cartesian => Ok(data.validate_cached()?.cloned()),
        TopologyDomain::Geographic(frame) => {
            if let Some(issue) = data.validate_structure_cached() {
                return Ok(Some(issue.clone()));
            }
            let view = match geographic_validation_view(data.shape(), frame)? {
                GeographicValidationView::Admitted(view) => view,
                GeographicValidationView::Issue(issue) => return Ok(Some(issue)),
            };
            if matches!(data.shape(), Shape::GeometryCollection(_)) {
                return validate_admitted_shape(data.shape(), frame, view, "$");
            }
            let route = data.geographic_validity_route(view);
            validate_routed_data(data, view, route, "$")
        },
    }
}

fn admitted_geographic_view(
    shape: &Shape,
    frame: GeographicFrame,
) -> Result<ScalarGeographicView<'_>> {
    ScalarGeographicView::try_new(frame, shape)
        .map_err(|diagnostic| geographic_admission_error(&diagnostic))
}

fn simple_on_quotient_chart(data: &PreparedPlan, view: ScalarGeographicView<'_>) -> Result<bool> {
    match data.shape() {
        Shape::LineString(_) | Shape::MultiLineString(_) => {
            let shape = data.shape();
            data.geographic_exact_topology(view)?
                .self_intersection_points(shape)
                .map(|points| points.is_empty())
        },
        Shape::Polygon(_) | Shape::MultiPolygon(_) => {
            Ok(validate_data_in_frame(data, TopologyDomain::Geographic(view.frame()))?.is_none())
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => data.is_simple_cached(),
    }
}

/// Frame-aware simplicity. Exact-partition charts reuse the cached planar
/// verdict; quotient charts use the frame-aware polygon validator.
pub(crate) fn is_simple_data_in_frame(data: &PreparedPlan, domain: TopologyDomain) -> Result<bool> {
    let TopologyDomain::Geographic(frame) = domain else {
        return data.is_simple_cached();
    };
    let view = admitted_geographic_view(data.shape(), frame)?;
    match data.geographic_validity_route(view) {
        crate::geometry::GeographicTopologyRoute::ExactPartition => data.is_simple_cached(),
        crate::geometry::GeographicTopologyRoute::Quotient => simple_on_quotient_chart(data, view),
    }
}

/// Frame-aware `is_ring`: closure compares the source endpoints in the frame's
/// vertex identity, and quotient-chart simplicity reads exact Q incidence.
pub(crate) fn is_ring_data_in_frame(data: &PreparedPlan, domain: TopologyDomain) -> Result<bool> {
    let TopologyDomain::Geographic(frame) = domain else {
        return data.shape().is_ring();
    };
    let view = admitted_geographic_view(data.shape(), frame)?;
    if !matches!(data.shape(), Shape::LineString(_))
        || !crate::geometry::shape_is_closed_in_domain(data.shape(), domain)
    {
        return Ok(false);
    }
    match data.geographic_validity_route(view) {
        crate::geometry::GeographicTopologyRoute::ExactPartition => data.is_simple_cached(),
        crate::geometry::GeographicTopologyRoute::Quotient => simple_on_quotient_chart(data, view),
    }
}

/// Frame-aware self-intersection diagnostics, shared by scalar and grouped
/// array output.
#[cfg(test)]
pub(crate) fn self_intersections_in_frame(
    shape: &Shape,
    domain: TopologyDomain,
) -> Result<Vec<Point>> {
    let TopologyDomain::Geographic(frame) = domain else {
        return shape.self_intersections();
    };
    let view = admitted_geographic_view(shape, frame)?;
    match crate::geometry::classify_geographic_validity_chart(view) {
        crate::geometry::GeographicTopologyRoute::ExactPartition => shape.self_intersections(),
        crate::geometry::GeographicTopologyRoute::Quotient => {
            ExactGeographicTopology::from_views(frame, std::iter::once(view))?
                .self_intersection_points(shape)
        },
    }
}

pub(crate) fn self_intersections_data_in_frame(
    data: &PreparedPlan,
    domain: TopologyDomain,
) -> Result<Vec<Point>> {
    let TopologyDomain::Geographic(frame) = domain else {
        return data.shape().self_intersections();
    };
    let view = admitted_geographic_view(data.shape(), frame)?;
    match data.geographic_validity_route(view) {
        crate::geometry::GeographicTopologyRoute::ExactPartition => {
            data.shape().self_intersections()
        },
        crate::geometry::GeographicTopologyRoute::Quotient => data
            .geographic_diagnostic_topology(view)?
            .self_intersection_points(data.shape()),
    }
}

fn repair_invalid_shape(
    shape: &Shape,
    domain: TopologyDomain,
    method: RepairMethod,
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    let repair = |shape: &Shape| match domain {
        TopologyDomain::Cartesian => shape.repair_with_context(method, false, context),
        TopologyDomain::Geographic(_) => {
            shape.repair_preserving_source_vertices(method, false, context)
        },
    };
    let repaired = if needs_topology_split(shape, domain) {
        // Preserve source Z/M on ordinary seam crossings. Polar closure with
        // Z/M remains an honest error: no source ordinate exists at a
        // fabricated pole vertex.
        let split = split_in_domain(shape, domain, context)?;
        repair(&split)
    } else {
        repair(shape)
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
    context: &SourceArcContext<'_, '_>,
) -> Result<Option<Shape>> {
    let Some(issue) = validate_shape_in_frame(shape, domain)? else {
        return Ok(None);
    };
    reject_ambiguous_ring_repair(&issue)?;
    repair_invalid_shape(shape, domain, method, context).map(Some)
}

/// Cached-handle twin of [`repair_shape_in_frame`].
pub(crate) fn repair_data_in_frame(
    data: &PreparedPlan,
    domain: TopologyDomain,
    method: RepairMethod,
    context: &SourceArcContext<'_, '_>,
) -> Result<Option<Shape>> {
    let Some(issue) = validate_data_in_frame(data, domain)? else {
        return Ok(None);
    };
    reject_ambiguous_ring_repair(&issue)?;
    repair_invalid_shape(data.shape(), domain, method, context).map(Some)
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
        .map(crate::geographic::CanonicalLongitude::get)
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
/// * `interior` callers (`point_on_surface`, which must return a point in or on
///   the geometry) AND any pole-enclosing geometry route through the seam+pole
///   SPLIT — the unwrapped form of a polar cap is a degenerate longitude sliver
///   (a longitude shift can't reconstruct the cap), and the split's planar
///   kernel yields a point in the true region. The split is in the frame's
///   canonical longitude interval, so no rewrap is needed.
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
    let encloses_pole = shape_encloses_pole_in_frame(shape, frame, ChartPole::North)
        || shape_encloses_pole_in_frame(shape, frame, ChartPole::South);
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
    if let TopologyDomain::Geographic(frame) = domain
        && matches!(strategy, DerivedPointStrategy::Interior)
        && let Some(point) = point_set_surface(shape, frame)?
    {
        return Ok(point);
    }
    if let TopologyDomain::Geographic(frame) = domain
        && matches!(strategy, DerivedPointStrategy::Centroid)
        && matches!(
            shape,
            Shape::MultiLineString(_) | Shape::MultiPolygon(_) | Shape::GeometryCollection(_)
        )
        && crate::geometry::crosses_antimeridian_in_frame(shape, frame)
    {
        let split = try_topology_split(shape, domain)?;
        return if derived_point_needs_unwrap(&split, frame) {
            derived_point_unwrapped(&split, frame, strategy, kernel)
        } else {
            kernel(&split)
        };
    }
    match domain {
        TopologyDomain::Geographic(frame) if derived_point_needs_unwrap(shape, frame) => {
            derived_point_unwrapped(shape, frame, strategy, kernel)
        },
        TopologyDomain::Cartesian | TopologyDomain::Geographic(_) => kernel(shape),
    }
}

pub(crate) fn unary_antimeridian_centroid(shape: &Shape, domain: TopologyDomain) -> Result<Shape> {
    let TopologyDomain::Geographic(frame) = domain else {
        return shape.centroid();
    };
    if let Some(point) = point_set_centroid(shape, frame)? {
        return Ok(point);
    }
    if !matches!(shape, Shape::Polygon(_) | Shape::MultiPolygon(_)) {
        return unary_antimeridian_derived(
            shape,
            domain,
            DerivedPointStrategy::Centroid,
            Shape::centroid,
        );
    }
    // Preserve the established crossing-first error order if the direct
    // centroid cannot produce a result.
    let Ok((centroid, bounds)) = shape.centroid_with_bounds() else {
        return unary_antimeridian_derived(
            shape,
            domain,
            DerivedPointStrategy::Centroid,
            Shape::centroid,
        );
    };
    if bounds.is_some_and(|bounds| frame.bounds_certify_ordinary_support(bounds)) {
        return Ok(centroid);
    }
    if matches!(shape, Shape::MultiPolygon(_))
        && crate::geometry::crosses_antimeridian_in_frame(shape, frame)
    {
        return unary_antimeridian_derived(
            shape,
            domain,
            DerivedPointStrategy::Centroid,
            Shape::centroid,
        );
    }
    if derived_point_needs_unwrap(shape, frame) {
        derived_point_unwrapped(
            shape,
            frame,
            DerivedPointStrategy::Centroid,
            Shape::centroid,
        )
    } else {
        Ok(centroid)
    }
}

/// Geographic support bounds from the shared periodic envelope owner.
pub(crate) fn geographic_crossing_bounds(shape: &Shape) -> Option<Bounds> {
    let frame = GeographicFrame::crs84();
    GeographicSupportEnvelope::try_from_shape(frame, shape)
        .ok()
        .and_then(|support| support.bounds(frame))
}

#[cfg(test)]
mod validity_tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::geographic::AngularFactor;
    use crate::geometry::test_support::{frame, ring};

    #[test]
    fn out_of_domain_latitude_is_a_validity_issue_but_not_a_repairable_input() {
        let frame = frame();
        let domain = TopologyDomain::Geographic(frame);
        let shape = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (10.0, 0.0), (10.0, 100.0), (0.0, 0.0)]),
            Vec::new(),
        ));
        let issue = validate_shape_in_frame(&shape, domain)
            .unwrap()
            .expect("out-of-domain latitude is invalid");
        assert_eq!(issue.code, "latitude_out_of_range");
        assert_eq!(issue.path.as_deref(), Some("$.shell"));
        assert_eq!(
            issue.location.unwrap().xy(),
            crate::geometry::XY::new(10.0, 100.0)
        );

        let prepared = PreparedPlan::transient(&shape);
        assert_eq!(
            validate_data_in_frame(&prepared, domain)
                .unwrap()
                .map(|issue| issue.code),
            Some("latitude_out_of_range")
        );
        let repair = repair_shape_in_frame(
            &shape,
            domain,
            RepairMethod::Linework,
            &SourceArcContext::new(domain, None),
        )
        .expect_err("geographic admission failures cannot be repaired");
        assert!(matches!(
            repair.kind(),
            ErrorKind::Geometry(GeometryErrorKind::RepairFailed(_))
        ));
        assert!(crate::crs::ensure_geographic_domain_in_frame(frame, &shape).is_err());
    }

    #[test]
    fn prepared_quotient_validity_reuses_exact_topology_for_simplicity() {
        let shape = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, 0.0),
                (-170.0, 0.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ]),
            Vec::new(),
        ));
        let plan = PreparedPlan::new(shape);
        let domain = TopologyDomain::Geographic(frame());

        assert!(validate_data_in_frame(&plan, domain).unwrap().is_none());
        assert!(validate_data_in_frame(&plan, domain).unwrap().is_none());
        assert!(is_simple_data_in_frame(&plan, domain).unwrap());
        assert!(
            self_intersections_data_in_frame(&plan, domain)
                .unwrap()
                .is_empty()
        );
        assert_eq!(plan.geographic_exact_topology_accesses(), 2);
        assert_eq!(plan.geographic_exact_topology_builds(), 1);
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
    )]
    fn prepared_diagnostics_preserve_repeated_collection_occurrences() {
        let line = Shape::LineString(
            crate::geometry::LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(170.0, 0.0),
                    Point::new_raw_xy(-170.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let owner = Shape::geometry_collection(vec![line]).unwrap();
        let shape = Shape::geometry_collection(vec![owner.clone(), owner]).unwrap();
        let domain = TopologyDomain::Geographic(frame());
        let expected = self_intersections_in_frame(&shape, domain).unwrap();
        assert!(expected.iter().any(|point| point.x == 170.0));
        assert!(expected.iter().any(|point| point.x == -170.0));
        let plan = PreparedPlan::new(shape);
        for _ in 0..2 {
            assert_eq!(
                self_intersections_data_in_frame(&plan, domain).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn prepared_validity_verdict_is_keyed_by_geographic_frame() {
        let plan = PreparedPlan::new(Shape::Point(Point::new_raw_xy(0.0, 95.0)));
        let degrees = TopologyDomain::Geographic(frame());
        let grads = AngularFactor::try_new("grads", std::f64::consts::TAU / 400.0).unwrap();
        let grads =
            TopologyDomain::Geographic(GeographicFrame::try_new(grads, grads, 0.0).unwrap());
        for domain in [degrees, grads, degrees] {
            let issue = validate_data_in_frame(&plan, domain).unwrap();
            if domain == degrees {
                assert_eq!(issue.unwrap().code, "latitude_out_of_range");
            } else {
                assert!(issue.is_none());
            }
        }
    }

    #[test]
    fn structural_ring_issue_precedes_geographic_admission() {
        let shape = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (10.0, 100.0), (0.0, 0.0), (0.0, 0.0)]),
            Vec::new(),
        ));
        let issue = validate_shape_in_frame(&shape, TopologyDomain::Geographic(frame()))
            .unwrap()
            .expect("the stored ring is structurally collapsed");
        assert_eq!(issue.code, "ring_collapsed");
    }
}
