use super::{
    GeographicFrame, MOrdinate, Point, PyErr, PyResult, Shape, ZOrdinate, crs, geographic,
    same_topological_coordinate,
};

pub(super) fn to_geodesic_point(frame: GeographicFrame, point: Point) -> PyResult<Point> {
    frame
        .to_geodesic_point(point)
        .map_err(|_| geographic::invalid_lonlat_error(point.x, point.y).into())
}

pub(super) fn point_from_geodesic(frame: GeographicFrame, point: Point) -> PyResult<Point> {
    frame
        .point_from_geodesic(point)
        .map_err(|error| PyErr::from(crs::CrsError::message(error.to_string())))
}

pub(super) fn bearing_degrees(dx: f64, dy: f64) -> f64 {
    let bearing = dx.atan2(dy).to_degrees();
    if bearing < 0.0 {
        bearing + 360.0
    } else {
        bearing
    }
}

pub(super) fn distance_rhumb_points(
    crs: &str,
    frame: GeographicFrame,
    from: Point,
    to: Point,
    state: &crs::RuntimeState,
) -> PyResult<f64> {
    let from = to_geodesic_point(frame, from)?;
    let to = to_geodesic_point(frame, to)?;
    Ok(crs::distance_rhumb_crs_at(
        crs, from.x, from.y, to.x, to.y, state,
    )?)
}

pub(super) fn rhumb_bearing_points(
    crs: &str,
    frame: GeographicFrame,
    from: Point,
    to: Point,
    state: &crs::RuntimeState,
) -> PyResult<f64> {
    let from = to_geodesic_point(frame, from)?;
    let to = to_geodesic_point(frame, to)?;
    Ok(crs::rhumb_bearing_crs_at(
        crs, from.x, from.y, to.x, to.y, state,
    )?)
}

pub(super) fn destination_point_planar(
    to_metre: f64,
    from: Point,
    bearing: f64,
    distance: f64,
) -> PyResult<Point> {
    if same_topological_coordinate(distance, 0.0) {
        return Ok(from);
    }
    let (sine, cosine) = crate::crs::sincos_degrees(bearing, 0.0);
    let step = distance / to_metre;
    Ok(Point::new_axes(
        step * sine + from.x,
        step * cosine + from.y,
        ZOrdinate(from.z()),
        MOrdinate(from.m()),
    )?)
}

pub(super) fn destination_point(
    model: &crs::MetricModel,
    from: Point,
    bearing: f64,
    distance: f64,
) -> PyResult<Point> {
    match model {
        crs::MetricModel::Geodesic { crs, frame, state } => {
            if same_topological_coordinate(distance, 0.0) {
                to_geodesic_point(*frame, from)?;
                return Ok(from);
            }
            let from_geodesic = to_geodesic_point(*frame, from)?;
            let (x, y) = crs::geodesic_destination_crs_at(
                crs,
                from_geodesic.x,
                from_geodesic.y,
                bearing,
                distance,
                state,
            )?;
            point_from_geodesic(
                *frame,
                Point::new_axes(x, y, ZOrdinate(from.z()), MOrdinate(from.m()))?,
            )
        },
        crs::MetricModel::Planar { to_metre } => {
            destination_point_planar(to_metre.get(), from, bearing, distance)
        },
    }
}

pub(super) fn destination_point_on_geodesic(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    from: Point,
    bearing: f64,
    distance: f64,
) -> crate::error::Result<Point> {
    if same_topological_coordinate(distance, 0.0) {
        frame
            .to_geodesic_point(from)
            .map_err(|_| geographic::invalid_lonlat_error(from.x, from.y))?;
        return Ok(from);
    }
    let from_geodesic = frame
        .to_geodesic_point(from)
        .map_err(|_| geographic::invalid_lonlat_error(from.x, from.y))?;
    let (x, y) = crs::geodesic_destination(
        geodesic,
        from_geodesic.x,
        from_geodesic.y,
        bearing,
        distance,
    )?;
    let point = Point::new_axes(x, y, ZOrdinate(from.z()), MOrdinate(from.m()))?;
    frame
        .point_from_geodesic(point)
        .map_err(|error| crate::crs::CrsError::message(error.to_string()))
}

pub(super) fn rhumb_destination_point(
    crs: &str,
    frame: GeographicFrame,
    from: Point,
    bearing: f64,
    distance: f64,
    state: &crs::RuntimeState,
) -> PyResult<Shape> {
    if same_topological_coordinate(distance, 0.0) {
        to_geodesic_point(frame, from)?;
        return Ok(Shape::Point(from));
    }
    let from_geodesic = to_geodesic_point(frame, from)?;
    let Some((x, y)) = crs::rhumb_destination_crs_at(
        crs,
        from_geodesic.x,
        from_geodesic.y,
        bearing,
        distance,
        state,
    )?
    else {
        return Err(crate::geometry::GeometryErrorKind::message(
            "destination(path='rhumb') crosses a pole, where the destination longitude is \
             indeterminate",
        )
        .into());
    };
    Ok(Shape::Point(point_from_geodesic(
        frame,
        Point::new_axes(x, y, ZOrdinate(from.z()), MOrdinate(from.m()))?,
    )?))
}

pub(super) fn interpolate_point_on_geodesic(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    from: Point,
    to: Point,
    distance: f64,
    normalized: bool,
) -> crate::error::Result<Point> {
    let from_geodesic = frame
        .to_geodesic_point(from)
        .map_err(|_| geographic::invalid_lonlat_error(from.x, from.y))?;
    let to_geodesic = frame
        .to_geodesic_point(to)
        .map_err(|_| geographic::invalid_lonlat_error(to.x, to.y))?;
    // One inverse (dist+az) + optional direct — total distance is never
    // solved as a separate inverse first.
    let (x, y, ratio) = crs::geodesic_point_between(
        geodesic,
        from_geodesic.x,
        from_geodesic.y,
        to_geodesic.x,
        to_geodesic.y,
        distance,
        normalized,
    )?;
    if same_topological_coordinate(ratio, 0.0) {
        return Ok(from);
    }
    if same_topological_coordinate(ratio, 1.0) {
        return Ok(to);
    }
    let point = Point::new_axes(
        x,
        y,
        ZOrdinate(crate::geometry::interpolate_optional_ordinate(
            from.z(),
            to.z(),
            ratio,
        )),
        MOrdinate(crate::geometry::interpolate_optional_ordinate(
            from.m(),
            to.m(),
            ratio,
        )),
    )?;
    frame
        .point_from_geodesic(point)
        .map_err(|error| crate::crs::CrsError::message(error.to_string()))
}

pub(super) fn interpolate_point_planar(
    to_metre: f64,
    from: Point,
    to: Point,
    distance: f64,
    normalized: bool,
) -> Shape {
    let total = (to.x - from.x).hypot(to.y - from.y) * to_metre;
    let ratio = if normalized {
        distance
    } else if total == 0.0 {
        0.0
    } else {
        distance / total
    }
    .clamp(0.0, 1.0);
    if same_topological_coordinate(ratio, 0.0) {
        return Shape::Point(from);
    }
    if same_topological_coordinate(ratio, 1.0) {
        return Shape::Point(to);
    }
    // Stable lerp: `start*(1-t)+end*t` avoids the `(end-start)*t`
    // overflow class on extreme-but-finite endpoints.
    Shape::Point(crate::geometry::lerp_point(from, to, ratio))
}

pub(super) fn interpolate_point(
    model: &crs::MetricModel,
    from: Point,
    to: Point,
    distance: f64,
    normalized: bool,
) -> PyResult<Shape> {
    match model {
        crs::MetricModel::Geodesic { crs, frame, state } => {
            let geodesic = crs::geodesic_for_crs_at(crs, state)?;
            Ok(Shape::Point(interpolate_point_on_geodesic(
                &geodesic, *frame, from, to, distance, normalized,
            )?))
        },
        crs::MetricModel::Planar { to_metre } => Ok(interpolate_point_planar(
            to_metre.get(),
            from,
            to,
            distance,
            normalized,
        )),
    }
}

pub(super) fn rhumb_interpolate_point(
    crs: &str,
    frame: GeographicFrame,
    from: Point,
    to: Point,
    distance: f64,
    normalized: bool,
    state: &crs::RuntimeState,
) -> PyResult<Shape> {
    let from_geodesic = to_geodesic_point(frame, from)?;
    let to_geodesic = to_geodesic_point(frame, to)?;
    let total = crs::distance_rhumb_crs_at(
        crs,
        from_geodesic.x,
        from_geodesic.y,
        to_geodesic.x,
        to_geodesic.y,
        state,
    )?;
    let ratio = if normalized {
        distance
    } else if total == 0.0 {
        0.0
    } else {
        distance / total
    }
    .clamp(0.0, 1.0);
    if same_topological_coordinate(ratio, 0.0) {
        return Ok(Shape::Point(from));
    }
    if same_topological_coordinate(ratio, 1.0) {
        return Ok(Shape::Point(to));
    }
    let bearing = crs::rhumb_bearing_crs_at(
        crs,
        from_geodesic.x,
        from_geodesic.y,
        to_geodesic.x,
        to_geodesic.y,
        state,
    )?;
    let Some((x, y)) = crs::rhumb_destination_crs_at(
        crs,
        from_geodesic.x,
        from_geodesic.y,
        bearing,
        total * ratio,
        state,
    )?
    else {
        return Err(crate::geometry::GeometryErrorKind::message(
            "point_between(path='rhumb') crosses a pole, where the destination longitude is \
             indeterminate",
        )
        .into());
    };
    Ok(Shape::Point(point_from_geodesic(
        frame,
        Point::new_axes(
            x,
            y,
            ZOrdinate(crate::geometry::interpolate_optional_ordinate(
                from.z(),
                to.z(),
                ratio,
            )),
            MOrdinate(crate::geometry::interpolate_optional_ordinate(
                from.m(),
                to.m(),
                ratio,
            )),
        )?,
    )?))
}

#[expect(
    clippy::float_cmp,
    reason = "geographic quotient identity and exact antipodes have no epsilon neighborhood"
)]
pub(super) fn cross_track_endpoints(
    frame: GeographicFrame,
    from: Point,
    to: Point,
) -> PyResult<(Point, Point)> {
    let from_geodesic = to_geodesic_point(frame, from)?;
    let to_geodesic = to_geodesic_point(frame, to)?;
    let same_point = frame.quotient_vertex(from.x, from.y) == frame.quotient_vertex(to.x, to.y);
    let opposite_latitudes = from_geodesic.y == -to_geodesic.y;
    let opposite_poles = from_geodesic.y.abs() == 90.0;
    let opposite_longitudes = matches!(
        GeographicFrame::crs84()
            .longitude()
            .shortest_alias_step(from_geodesic.x, to_geodesic.x),
        Ok(crate::geographic::EdgeDirection::HalfTurn(_))
    );
    if same_point || (opposite_latitudes && (opposite_poles || opposite_longitudes)) {
        return Err(PyErr::from(crate::geometry::GeometryErrorKind::message(
            "cross_track_distance requires start and end points that define a unique great circle; coincident and antipodal endpoints are invalid",
        )));
    }
    Ok((from_geodesic, to_geodesic))
}
