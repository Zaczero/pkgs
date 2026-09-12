use crate::error::{ResourceError, Result};
use crate::geographic::{
    GeographicFrame, GeographicRing, GeographicViewError, Latitude, RingDomain, RingSummary,
    RingTurn, ScalarGeographicView,
};
use crate::geometry::antimeridian::{
    collect_points, dedup_consecutive, resolve_seam_aliases, segment_coords, segment_coords_exact,
};
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates as _, GeometryErrorKind, LineSeq, MOrdinate,
    MultiLineString, MultiMember, MultiPolygon, Point, Polygon, Ring, Shape, Strictness, ZOrdinate,
    open_point_cycle_winding, polygon_is_valid, ring_encloses_pole, ring_winding, same_point,
};

impl Shape {
    /// Validate and split in the periodic coordinate frame. Canonical chart
    /// coordinates exist only for this operation; source coordinates remain
    /// the storage authority.
    pub(crate) fn split_antimeridian_in_frame(&self, frame: GeographicFrame) -> Result<Self> {
        let view = ScalarGeographicView::try_new(frame, self).map_err(split_view_error)?;
        split_geographic_view(view)
    }

    pub(crate) fn split_antimeridian_in_frame_exact(&self, frame: GeographicFrame) -> Result<Self> {
        let view = ScalarGeographicView::try_new(frame, self).map_err(split_view_error)?;
        split_geographic_view_exact(view)
    }

    pub fn split_antimeridian(&self) -> Result<Self> {
        let degrees =
            crate::geographic::AngularFactor::try_new("angular unit", std::f64::consts::PI / 180.0)
                .expect("degree angular factor is valid");
        let frame = GeographicFrame::try_new(degrees, degrees, 0.0)
            .expect("degree geographic frame is valid");
        self.split_antimeridian_in_frame(frame)
    }

    /// Split at the antimeridian: parts that cross ±180 come back as
    /// multiple parts whose edges follow the seam (each side keeping its
    /// own seam sign), with great-circle crossing latitudes and automatic
    /// pole closure. Geometries that do not cross are returned unchanged.
    /// The caller supplies an already-admitted canonical operation chart.
    fn split_antimeridian_chart_with_precision(&self, exact_crossings: bool) -> Result<Self> {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => Ok(self.clone()),
            Self::LineString(points) => {
                let pieces = seam_segments(&collect_points(points), exact_crossings);
                if pieces.is_empty() {
                    Ok(self.clone())
                } else {
                    Self::multi_line_string(
                        CoordinateAxes::XY,
                        pieces
                            .into_iter()
                            .map(|piece| CoordSeq::try_from_points(&piece))
                            // Splitting a line at meridian crossings yields
                            // pieces of at least two vertices each.
                            .map(|coords| coords.map(LineSeq::from_proven))
                            .collect::<Result<Vec<_>>>()?,
                    )
                }
            },
            Self::MultiLineString(lines) => {
                let mut split = Vec::with_capacity(lines.len());
                for member in lines.members() {
                    match member {
                        MultiMember::Empty => split.push(MultiMember::Empty),
                        MultiMember::Value(index) => {
                            let line = &lines.as_slice()[index];
                            let pieces = seam_segments(&collect_points(line), exact_crossings);
                            if pieces.is_empty() {
                                split.push(MultiMember::Value(line.clone()));
                            } else {
                                let lines = pieces
                                    .into_iter()
                                    .map(|piece| CoordSeq::try_from_points(&piece))
                                    .map(|coords| coords.map(LineSeq::from_proven))
                                    .map(|line| line.map(MultiMember::Value))
                                    .collect::<Result<Vec<_>>>()?;
                                split.extend(lines);
                            }
                        },
                    }
                }
                Ok(Self::MultiLineString(MultiLineString::try_new(
                    lines.axes(),
                    split,
                )?))
            },
            Self::Polygon(polygon) => {
                let polygons = split_polygon(self, polygon, PoleClosure::Auto, exact_crossings)?;
                Ok(Self::narrow_polygons(polygon.axes(), polygons)?)
            },
            Self::MultiPolygon(polygons) => {
                let mut split = Vec::with_capacity(polygons.len());
                for member in polygons.members() {
                    match member {
                        MultiMember::Empty => split.push(MultiMember::Empty),
                        MultiMember::Value(index) => {
                            split.extend(
                                split_polygon(
                                    self,
                                    &polygons.as_slice()[index],
                                    PoleClosure::Auto,
                                    exact_crossings,
                                )?
                                .into_iter()
                                .map(MultiMember::Value),
                            );
                        },
                    }
                }
                Ok(Self::MultiPolygon(MultiPolygon::try_new(
                    polygons.axes(),
                    split,
                )?))
            },
            Self::GeometryCollection(geometries) => {
                Ok(Self::GeometryCollection(geometries.try_map_members(
                    |member| member.split_antimeridian_chart_with_precision(exact_crossings),
                )?))
            },
        }
    }

    /// Split at the antimeridian forcing closure over a specific pole, for
    /// callers that already know which pole a ring encircles. A region that
    /// *contains* a pole (e.g. a polar cell coverage) has a ring edge spanning
    /// `> 180°` of longitude that the automatic inference can close over the
    /// wrong pole — producing a valid but wrong-region polygon the `Auto` retry
    /// (which only re-tries on *invalid* output) never corrects. Non-areal and
    /// non-crossing geometries behave exactly as [`Self::split_antimeridian`].
    ///
    /// Contract: each part must genuinely touch the forced pole. The closure
    /// assumes the regular seam topology of a single pole-touching ring.
    /// Forcing the wrong pole, or a ring that does not reach the forced pole,
    /// is caller error and yields garbage.
    pub fn split_antimeridian_over_pole(&self, north: bool) -> Result<Self> {
        let closure = if north {
            PoleClosure::North
        } else {
            PoleClosure::South
        };
        match self {
            Self::Polygon(polygon) => {
                let polygons = split_polygon(self, polygon, closure, false)?;
                Ok(Self::narrow_polygons(polygon.axes(), polygons)?)
            },
            Self::MultiPolygon(polygons) => {
                let mut split = Vec::with_capacity(polygons.len());
                for member in polygons.members() {
                    match member {
                        MultiMember::Empty => split.push(MultiMember::Empty),
                        MultiMember::Value(index) => {
                            split.extend(
                                split_polygon(self, &polygons.as_slice()[index], closure, false)?
                                    .into_iter()
                                    .map(MultiMember::Value),
                            );
                        },
                    }
                }
                Ok(Self::MultiPolygon(MultiPolygon::try_new(
                    polygons.axes(),
                    split,
                )?))
            },
            _ => self.split_antimeridian(),
        }
    }
}

fn split_view_error(error: GeographicViewError) -> crate::error::Error {
    match error {
        GeographicViewError::Resource => ResourceError::Allocation.into(),
        GeographicViewError::Diagnostic(diagnostic) => {
            GeometryErrorKind::antimeridian_split_failed(diagnostic.to_string())
        },
    }
}

fn seam_segments(points: &[Point], exact_crossings: bool) -> Vec<Vec<Point>> {
    if exact_crossings {
        segment_coords_exact(points)
    } else {
        segment_coords(points)
    }
}

fn source_longitudes_are_canonical(view: ScalarGeographicView<'_>) -> bool {
    let frame = view.frame();
    let mut canonical = true;
    view.for_each_coordinate(|coordinate| {
        let native = coordinate.native().x;
        let wrapped = coordinate.longitude().get();
        let positive_seam = native.to_bits() == frame.longitude().half_period().to_bits();
        let ordinary_identity = native.to_bits() == wrapped.to_bits()
            || (native.to_bits() == (-0.0_f64).to_bits() && wrapped.to_bits() == 0.0_f64.to_bits());
        canonical &= positive_seam || ordinary_identity;
    });
    canonical
}

pub(crate) fn split_geographic_view(view: ScalarGeographicView<'_>) -> Result<Shape> {
    split_geographic_view_with_precision(view, false)
}

pub(crate) fn split_geographic_view_exact(view: ScalarGeographicView<'_>) -> Result<Shape> {
    split_geographic_view_with_precision(
        view,
        shape_needs_exact_overlay_crossings(view.shape(), view.frame()),
    )
}

fn split_geographic_view_with_precision(
    view: ScalarGeographicView<'_>,
    exact_crossings: bool,
) -> Result<Shape> {
    let frame = view.frame();
    let canonical = source_longitudes_are_canonical(view);
    if frame == GeographicFrame::crs84() && canonical && !view.shape().crosses_antimeridian() {
        return Ok(view.shape().clone());
    }
    let longitude_scale = frame.longitude().unit().radians_per_unit().to_degrees();
    let latitude_scale = frame.latitude_unit().radians_per_unit().to_degrees();
    let chart = chart_shape(view.shape(), frame, longitude_scale, latitude_scale)?;
    if canonical && !chart.crosses_antimeridian() {
        return Ok(view.shape().clone());
    }
    let split = chart.split_antimeridian_chart_with_precision(exact_crossings)?;
    split.map_xy_with(|x, y| {
        let latitude = if y == 90.0 {
            frame.pole()
        } else if y == -90.0 {
            -frame.pole()
        } else {
            y / latitude_scale
        };
        Ok((x / longitude_scale, latitude))
    })
}

fn shape_needs_exact_overlay_crossings(shape: &Shape, frame: GeographicFrame) -> bool {
    match shape {
        Shape::Polygon(polygon) => polygon_needs_exact_overlay_crossings(polygon, frame),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .any(|polygon| polygon_needs_exact_overlay_crossings(polygon, frame)),
        Shape::GeometryCollection(geometries) => geometries
            .iter()
            .any(|geometry| shape_needs_exact_overlay_crossings(geometry, frame)),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => false,
    }
}

fn polygon_needs_exact_overlay_crossings(polygon: &Polygon, frame: GeographicFrame) -> bool {
    let axis = frame.longitude();
    polygon.rings().any(|ring| {
        let (min_y, max_y) = ring
            .iter_coords()
            .map(|point| point.y)
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), y| {
                (min.min(y), max.max(y))
            });
        let max_piece_length = ring
            .segment_pairs()
            .filter_map(|[start, end]| {
                let raw_delta = (end.x - start.x).abs();
                if raw_delta <= axis.half_period() || raw_delta >= axis.period() {
                    return None;
                }
                let shortest = axis
                    .shortest_edge(start.x, end.x)
                    .expect("validated geographic longitude")
                    .delta(axis)
                    .abs();
                Some(shortest * 0.5)
            })
            .fold(0.0, f64::max);
        max_piece_length > 0.0 && max_y - min_y <= 1e-7 * max_piece_length
    })
}

fn chart_point(
    point: Point,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
) -> Result<Point> {
    point.with_xy(
        chart_longitude(point.x, frame, longitude_scale)?,
        chart_latitude(point.y, frame, latitude_scale)?,
    )
}

fn chart_longitude(value: f64, frame: GeographicFrame, scale: f64) -> Result<f64> {
    let axis = frame.longitude();
    let remainder = value % axis.period();
    if remainder.to_bits() == axis.half_period().to_bits() {
        return Ok(180.0);
    }
    axis.canonicalize(value)
        .map(|longitude| longitude.get() * scale)
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

fn chart_latitude(value: f64, frame: GeographicFrame, scale: f64) -> Result<f64> {
    Latitude::try_new(frame, value)
        .map(|latitude| latitude.get() * scale)
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

fn chart_ring(
    ring: &Ring,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
) -> Result<Ring> {
    let geographic = GeographicRing::from_ring(ring);
    let summary = RingSummary::try_new(frame, &geographic).map_err(split_view_error)?;
    let mut points = Vec::new();
    points.reserve_exact(geographic.len());
    if matches!(summary.domain, RingDomain::FullLongitude(_))
        && matches!(summary.turn, RingTurn::None)
    {
        let xs = geographic.native_xs();
        let west = xs.iter().copied().fold(f64::INFINITY, f64::min);
        let east = xs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        for index in 0..geographic.len() {
            let point = geographic.native_point(index);
            let x = if point.x.to_bits() == west.to_bits() {
                -180.0
            } else if point.x.to_bits() == east.to_bits() {
                180.0
            } else {
                chart_longitude(point.x, frame, longitude_scale)?
            };
            points.push(point.with_xy(x, chart_latitude(point.y, frame, latitude_scale)?)?);
        }
    } else {
        for index in 0..geographic.len() {
            points.push(chart_point(
                geographic.native_point(index),
                frame,
                longitude_scale,
                latitude_scale,
            )?);
        }
    }
    Ok(Ring::from_closed(CoordSeq::try_from_points(&points)?))
}

fn chart_polygon(
    polygon: &Polygon,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
) -> Result<Polygon> {
    let shell = chart_ring(&polygon.shell, frame, longitude_scale, latitude_scale)?;
    let mut holes = Vec::new();
    holes.reserve_exact(polygon.holes.len());
    for hole in polygon.holes.iter() {
        holes.push(chart_ring(hole, frame, longitude_scale, latitude_scale)?);
    }
    Polygon::new(shell, holes)
}

fn chart_shape(
    shape: &Shape,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
) -> Result<Shape> {
    match shape {
        Shape::Polygon(polygon) => Ok(Shape::Polygon(chart_polygon(
            polygon,
            frame,
            longitude_scale,
            latitude_scale,
        )?)),
        Shape::MultiPolygon(polygons) => {
            let mut members = Vec::new();
            members.reserve_exact(polygons.members().len());
            for member in polygons.members() {
                members.push(match member {
                    MultiMember::Empty => MultiMember::Empty,
                    MultiMember::Value(index) => MultiMember::Value(chart_polygon(
                        &polygons.as_slice()[index],
                        frame,
                        longitude_scale,
                        latitude_scale,
                    )?),
                });
            }
            Ok(Shape::MultiPolygon(MultiPolygon::try_new(
                polygons.axes(),
                members,
            )?))
        },
        Shape::GeometryCollection(collection) => {
            Ok(Shape::GeometryCollection(collection.try_map_members(
                |member| chart_shape(member, frame, longitude_scale, latitude_scale),
            )?))
        },
        _ => shape.map_xy_with(|x, y| {
            Ok((
                chart_longitude(x, frame, longitude_scale)?,
                chart_latitude(y, frame, latitude_scale)?,
            ))
        }),
    }
}

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

fn ring_is_ccw(coords: &[Point]) -> bool {
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
fn split_polygon(
    source: &Shape,
    polygon: &Polygon,
    preferred: PoleClosure,
    exact_crossings: bool,
) -> Result<Vec<Polygon>> {
    let polygons = split_polygon_inner(polygon, preferred, exact_crossings)?;
    reject_fabricated_pole_ordinates(source, &polygons)?;
    Ok(polygons)
}

fn split_polygon_inner(
    polygon: &Polygon,
    preferred: PoleClosure,
    exact_crossings: bool,
) -> Result<Vec<Polygon>> {
    // Split crossing holes as independent areal operands, then subtract them
    // from the split shell.  Treating shell and hole seam fragments as one
    // undifferentiated stitching pool works for ordinary crossings, but it
    // cannot represent nested polar caps: both rings independently require a
    // pole closure, and the pooled closest-start walk joins them into invalid
    // self-intersecting rings.  Set difference is the structural operation the
    // source polygon already expresses, and the native overlay kernel gives us
    // the correct seam-connected annulus without a second stitching model.
    let mut crossing_holes = Vec::new();
    let mut stationary_holes = Vec::new();
    for hole in polygon.holes.iter() {
        if seam_segments(&collect_points(hole.coords()), exact_crossings).is_empty() {
            stationary_holes.push(hole.clone());
        } else {
            crossing_holes.push(hole.clone());
        }
    }
    if !crossing_holes.is_empty() {
        let axes = polygon.shell.coords().axes();
        let shell_only = Polygon::new(polygon.shell.clone(), stationary_holes)?;
        let shell_parts = split_polygon_inner(&shell_only, preferred, exact_crossings)?;
        let mut result = shape_from_polygons(shell_parts, axes)?;
        for hole in crossing_holes {
            let hole_axes = hole.coords().axes();
            let north = ring_encloses_pole(&hole, true);
            let south = ring_encloses_pole(&hole, false);
            let hole_closure = match (north, south) {
                (true, false) => PoleClosure::North,
                (false, true) => PoleClosure::South,
                _ => PoleClosure::Auto,
            };
            let hole_parts = split_polygon_inner(
                &Polygon::new(hole, Vec::new())?,
                hole_closure,
                exact_crossings,
            )?;
            let hole_shape = shape_from_polygons(hole_parts, hole_axes)?;
            result = result.difference(&hole_shape, Strictness::Strict)?;
        }
        return polygons_from_shape(result);
    }

    let mut exterior = collect_points(polygon.shell.coords());
    resolve_seam_aliases(&mut exterior);
    let exterior = dedup_consecutive(exterior);
    let mut segments = seam_segments(&exterior, exact_crossings);
    if segments.is_empty() {
        return Ok(vec![polygon.clone()]);
    }
    let mut kept_holes: Vec<&Ring> = Vec::new();
    for hole in polygon.holes.iter() {
        let coords = collect_points(hole.coords());
        let hole_segments = seam_segments(&coords, exact_crossings);
        if hole_segments.is_empty() {
            kept_holes.push(hole);
        } else {
            // A crossing hole participates in the seam stitching. Its
            // unwrapped winding must be CW (a hole); reverse when not.
            let unwrapped: Vec<Point> = coords
                .iter()
                .map(|point| {
                    let mut point = *point;
                    point.x = point.x.rem_euclid(360.0);
                    point
                })
                .collect();
            if ring_is_ccw(&unwrapped) {
                let reversed: Vec<Point> = coords.into_iter().rev().collect();
                segments.extend(seam_segments(&reversed, exact_crossings));
            } else {
                segments.extend(hole_segments);
            }
        }
    }
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
    for closure in [PoleClosure::North, PoleClosure::South] {
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

fn shape_from_polygons(polygons: Vec<Polygon>, axes: CoordinateAxes) -> Result<Shape> {
    Shape::narrow_polygons(axes, polygons)
}

fn polygons_from_shape(shape: Shape) -> Result<Vec<Polygon>> {
    match shape {
        Shape::Polygon(polygon) => Ok(vec![polygon]),
        Shape::MultiPolygon(polygons) => Ok(polygons.iter().cloned().collect()),
        Shape::Empty(..) => Ok(Vec::new()),
        _ => Err(GeometryErrorKind::antimeridian_split_failed(
            "subtracting crossing holes produced non-areal output",
        )),
    }
}

/// Build the final polygons from stitched seam segments: rings, the
/// single-ring pole-covering wrap, and kept-hole assignment.
fn assemble_polygons(segments: Vec<Vec<Point>>, kept_holes: &[&Ring]) -> Result<Vec<Polygon>> {
    let mut shells = build_rings(segments);
    if shells.is_empty() {
        return Err(GeometryErrorKind::antimeridian_split_failed(
            "no ring could be stitched along the seam",
        ));
    }
    // A single non-CCW result encloses both poles: represent it as the world
    // rectangle with the ring as a hole.
    if shells.len() == 1 && !ring_is_ccw(&shells[0]) {
        // The fabricated world rectangle carries the ring's axes so shell and
        // hole stay axis-consistent (a measured/3D pole wrap is rejected
        // downstream; the placeholder ordinate is never observed).
        let like = shells[0].first().copied().unwrap_or(Point::new(0.0, 0.0)?);
        let corner = |lon: f64, lat: f64| -> Result<Point> {
            Point::new_axes(lon, lat, ZOrdinate(like.z()), MOrdinate(like.m()))
        };
        let world = vec![
            corner(-180.0, 90.0)?,
            corner(-180.0, -90.0)?,
            corner(180.0, -90.0)?,
            corner(180.0, 90.0)?,
            corner(-180.0, 90.0)?,
        ];
        let hole = close_ring(shells.remove(0));
        return Ok(vec![Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&world)?),
            vec![Ring::from_closed(CoordSeq::try_from_points(&hole)?)],
        )?]);
    }
    let mut polygons = Vec::with_capacity(shells.len());
    for shell in shells {
        polygons.push(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&close_ring(shell))?),
            Vec::new(),
        )?);
    }
    // Assign each non-crossing hole to the part that contains it. Accumulate
    // every hole assigned to the same part.
    let candidate_shapes = polygons
        .iter()
        .cloned()
        .map(Shape::Polygon)
        .collect::<Vec<_>>();
    let mut assigned_holes = vec![Vec::<Ring>::new(); polygons.len()];
    for hole in kept_holes {
        let hole_shape = Shape::LineString(hole.as_line());
        let mut owner = None;
        for (index, candidate) in candidate_shapes.iter().enumerate() {
            if candidate.contains(&hole_shape)? {
                owner = Some(index);
                break;
            }
        }
        match owner {
            Some(index) => assigned_holes[index].push((*hole).clone()),
            None => {
                return Err(GeometryErrorKind::antimeridian_split_failed(
                    "a hole does not lie inside any split part",
                ));
            },
        }
    }
    for (polygon, mut holes) in polygons.iter_mut().zip(assigned_holes) {
        if holes.is_empty() {
            continue;
        }
        let combined = if polygon.holes.is_empty() {
            holes
        } else {
            let mut combined = polygon.holes.to_vec();
            combined.append(&mut holes);
            combined
        };
        *polygon = Polygon::new(polygon.shell.clone(), combined)?;
    }
    Ok(polygons)
}

fn close_ring(mut coords: Vec<Point>) -> Vec<Point> {
    if let (Some(first), Some(last)) = (coords.first().copied(), coords.last().copied())
        && !same_point(first, last)
    {
        coords.push(first);
    }
    coords
}

/// Which pole a stitched ring may close over: inferred from the seam
/// latitudes, or forced when the inference is ambiguous.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PoleClosure {
    Auto,
    North,
    South,
}

/// A seam end with no seam start between it and the pole closes over that pole.
/// Implying both poles under `Auto` means the winding was inverted, so the
/// original segments come back reversed. Returns the segments and whether any
/// pole closure was applied.
fn extend_over_poles(
    mut segments: Vec<Vec<Point>>,
    closure: PoleClosure,
) -> Result<(Vec<Vec<Point>>, bool)> {
    // A fabricated pole-cap vertex must carry the SAME axes as the chain it
    // extends, or the assembled ring would mix Z/M presence — violating the
    // CoordSeq axis-homogeneity invariant (a `from_points` debug-assert, and a
    // silently malformed sequence in release). The placeholder ordinate (copied
    // from the capped chain's endpoint) is never observed: a measured/3D ring
    // whose cap survives is rejected by `reject_fabricated_pole_ordinates`, and
    // an XY ring carries no ordinate to fabricate.
    let pole_vertex = |like: Point, lon: f64, lat: f64| -> Result<Point> {
        Point::new_axes(lon, lat, ZOrdinate(like.z()), MOrdinate(like.m()))
    };
    let mut left_start: Option<f64> = None;
    let mut right_start: Option<f64> = None;
    let mut left_end: Option<(usize, f64)> = None;
    let mut right_end: Option<(usize, f64)> = None;
    for (index, segment) in segments.iter().enumerate() {
        let start = segment[0];
        let end = *segment.last().expect("segments are non-empty");
        if start.x == -180.0 && left_start.is_none_or(|latitude| start.y < latitude) {
            left_start = Some(start.y);
        } else if start.x == 180.0 && right_start.is_none_or(|latitude| start.y > latitude) {
            right_start = Some(start.y);
        }
        if end.x == -180.0 && left_end.is_none_or(|(_, latitude)| end.y < latitude) {
            left_end = Some((index, end.y));
        } else if end.x == 180.0 && right_end.is_none_or(|(_, latitude)| end.y > latitude) {
            right_end = Some((index, end.y));
        }
    }
    let original = segments.clone();
    let mut over_north = false;
    let mut over_south = false;
    if let Some((index, latitude)) = left_end {
        if closure == PoleClosure::North
            && right_end.is_none()
            && left_start.is_none_or(|start| latitude > start)
        {
            // Forced north from the west side: cap and reverse.
            over_north = true;
            let like = *segments[index].last().expect("segments are non-empty");
            segments[index].push(pole_vertex(like, -180.0, 90.0)?);
            segments[index].push(pole_vertex(like, 180.0, 90.0)?);
            segments[index].reverse();
        } else if closure == PoleClosure::South || left_start.is_none_or(|start| latitude < start) {
            over_south = true;
            let like = *segments[index].last().expect("segments are non-empty");
            segments[index].push(pole_vertex(like, -180.0, -90.0)?);
            segments[index].push(pole_vertex(like, 180.0, -90.0)?);
        }
    }
    if let Some((index, latitude)) = right_end {
        if closure == PoleClosure::South && right_start.is_none_or(|start| latitude < start) {
            // Forced south from the east side: cap and reverse.
            over_south = true;
            let like = *segments[index].last().expect("segments are non-empty");
            segments[index].push(pole_vertex(like, 180.0, -90.0)?);
            segments[index].push(pole_vertex(like, -180.0, -90.0)?);
            segments[index].reverse();
        } else if closure == PoleClosure::North || right_start.is_none_or(|start| latitude > start)
        {
            over_north = true;
            let like = *segments[index].last().expect("segments are non-empty");
            segments[index].push(pole_vertex(like, 180.0, 90.0)?);
            segments[index].push(pole_vertex(like, -180.0, 90.0)?);
        }
    }
    let capped = over_north || over_south;
    if closure == PoleClosure::Auto && over_north && over_south {
        // Both poles implied: the input ring was wound backwards. Reverse
        // every original segment instead.
        let mut reversed = original;
        for segment in &mut reversed {
            segment.reverse();
        }
        return Ok((reversed, false));
    }
    Ok((segments, capped))
}

fn is_self_closing(segment: &[Point]) -> bool {
    let start = segment[0];
    let end = *segment.last().expect("segments are non-empty");
    let is_right = end.x == 180.0;
    start.x == end.x && ((is_right && start.y > end.y) || (!is_right && start.y < end.y))
}

/// Build polygons iteratively: pop a segment, find the closest seam start
/// toward the pole on the same side (including the donut clause), join and
/// repeat; a segment with no candidate closes on itself.
fn build_rings(mut segments: Vec<Vec<Point>>) -> Vec<Vec<Point>> {
    let mut rings = Vec::new();
    while let Some(mut segment) = segments.pop() {
        let end = *segment.last().expect("segments are non-empty");
        let start = segment[0];
        let is_right = end.x == 180.0;
        let mut best: Option<(usize, f64)> = None;
        let self_candidate = is_self_closing(&segment).then_some(start.y);
        for (index, candidate) in segments.iter().enumerate() {
            let candidate_start = candidate[0];
            let candidate_end = *candidate.last().expect("segments are non-empty");
            if candidate_start.x != end.x {
                continue;
            }
            let toward_pole = if is_right {
                candidate_start.y > end.y
                    && (!is_self_closing(candidate) || candidate_end.y < start.y)
            } else {
                candidate_start.y < end.y
                    && (!is_self_closing(candidate) || candidate_end.y > start.y)
            };
            if toward_pole {
                let better = best.is_none_or(|(_, latitude)| {
                    if is_right {
                        candidate_start.y < latitude
                    } else {
                        candidate_start.y > latitude
                    }
                });
                if better {
                    best = Some((index, candidate_start.y));
                }
            }
        }
        // The self-closing latitude competes with the joins: when it is
        // closer to the end than every candidate start, the segment closes
        // on itself.
        let join = match (best, self_candidate) {
            (Some((index, latitude)), Some(own)) => {
                let self_wins = if is_right {
                    own < latitude
                } else {
                    own > latitude
                };
                if self_wins { None } else { Some(index) }
            },
            (Some((index, _)), None) => Some(index),
            (None, _) => None,
        };
        if let Some(index) = join {
            let joined = segments.remove(index);
            segment.extend(joined);
            segments.push(segment);
            continue;
        }
        // Self-closing: drop fully degenerate single-point rings (an input
        // corner exactly on the seam).
        if !segment.iter().all(|point| same_point(*point, segment[0])) {
            rings.push(segment);
        }
    }
    rings
}

#[cfg(test)]
mod tests {
    use crate::geographic::{AngularFactor, GeographicFrame};
    use crate::geometry::{CoordSeq, LineSeq, MOrdinate, Point, Polygon, Ring, Shape, ZOrdinate};
    use crate::test_support::allocation_count;

    fn line(xs: &[f64], ys: &[f64]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(xs.to_vec(), ys.to_vec(), None, None).unwrap(),
            )
            .unwrap(),
        )
    }

    fn closed_ring(xs: &[f64], ys: &[f64]) -> Ring {
        Ring::closed_coordseq(
            CoordSeq::try_from_vecs(xs.to_vec(), ys.to_vec(), None, None).unwrap(),
        )
        .unwrap()
    }

    fn split_crs84(shape: &Shape) -> Shape {
        shape
            .split_antimeridian_in_frame(GeographicFrame::crs84())
            .unwrap()
    }

    #[test]
    fn degree_try_new_frame_equals_crs84() {
        let degrees = AngularFactor::try_new("angular unit", std::f64::consts::PI / 180.0).unwrap();
        let frame = GeographicFrame::try_new(degrees, degrees, 0.0).unwrap();
        assert_eq!(frame, GeographicFrame::crs84());
    }

    #[test]
    fn crs84_canonical_noncrossing_line_matches_source_and_skips_chart_alloc() {
        let shape = line(&[10.0, 20.0], &[1.0, 2.0]);
        let (cloned, clone_n) = allocation_count(|| shape.clone());
        let (split, split_n) = allocation_count(|| split_crs84(&shape));
        assert_eq!(split, cloned);
        assert_eq!(split_n, clone_n);
        let crossing = line(&[170.0, -170.0], &[0.0, 0.0]);
        let (_, crossing_n) = allocation_count(|| split_crs84(&crossing));
        assert!(clone_n < crossing_n);
    }

    #[test]
    fn crs84_half_period_endpoint_is_identity() {
        let shape = line(&[180.0, 0.0], &[0.0, 0.0]);
        assert_eq!(split_crs84(&shape), shape);
    }

    #[test]
    fn crs84_full_period_pair_is_identity() {
        let shape = line(&[-180.0, 180.0], &[0.0, 0.0]);
        assert_eq!(split_crs84(&shape), shape);
    }

    #[test]
    fn crs84_world_rectangle_is_identity() {
        let shape = Shape::Polygon(
            Polygon::new(
                closed_ring(&[-180.0, 180.0, 180.0, -180.0, -180.0], &[
                    -90.0, -90.0, 90.0, 90.0, -90.0,
                ]),
                Vec::new(),
            )
            .unwrap(),
        );
        assert_eq!(split_crs84(&shape), shape);
    }

    #[test]
    fn crs84_canonical_polygon_with_holes_is_identity() {
        let shape = Shape::Polygon(
            Polygon::new(
                closed_ring(&[0.0, 10.0, 10.0, 0.0, 0.0], &[0.0, 0.0, 10.0, 10.0, 0.0]),
                vec![closed_ring(&[2.0, 4.0, 4.0, 2.0, 2.0], &[
                    2.0, 2.0, 4.0, 4.0, 2.0,
                ])],
            )
            .unwrap(),
        );
        assert_eq!(split_crs84(&shape), shape);
    }

    #[test]
    fn crs84_nested_xyzm_collection_is_identity() {
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![1.0, 2.0],
                    vec![3.0, 4.0],
                    Some(vec![10.0, 20.0]),
                    Some(vec![7.0, 8.0]),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let point = Shape::Point(
            Point::new_axes(5.0, 6.0, ZOrdinate(Some(1.0)), MOrdinate(Some(2.0))).unwrap(),
        );
        let inner = Shape::geometry_collection(vec![point]).unwrap();
        let shape = Shape::geometry_collection(vec![line, inner]).unwrap();
        assert_eq!(split_crs84(&shape), shape);
    }

    #[test]
    fn crs84_zero_to_full_turn_band_rewrites() {
        let shape = Shape::Polygon(
            Polygon::new(
                closed_ring(&[0.0, 360.0, 360.0, 0.0, 0.0], &[
                    -30.0, -30.0, 30.0, 30.0, -30.0,
                ]),
                Vec::new(),
            )
            .unwrap(),
        );
        let split = split_crs84(&shape);
        assert_ne!(split, shape);
        let expected = Shape::Polygon(
            Polygon::new(
                closed_ring(&[-180.0, 180.0, 180.0, -180.0, -180.0], &[
                    -30.0, -30.0, 30.0, 30.0, -30.0,
                ]),
                Vec::new(),
            )
            .unwrap(),
        );
        assert_eq!(split, expected);
    }
}
