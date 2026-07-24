#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::error::Result;

impl Shape {
    /// Split at the antimeridian: parts that cross ±180 come back as
    /// multiple parts whose edges follow the seam (each side keeping its
    /// own seam sign), with great-circle crossing latitudes and automatic
    /// pole closure. Geometries that do not cross are returned unchanged.
    /// The caller validates the lon/lat domain.
    pub fn split_antimeridian(&self) -> Result<Self> {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => Ok(self.clone()),
            Self::LineString(points) => {
                let pieces = segment_coords(&collect_points(points));
                if pieces.is_empty() {
                    Ok(self.clone())
                } else {
                    Ok(Self::MultiLineString(
                        pieces
                            .into_iter()
                            .map(CoordSeq::from)
                            .map(|piece| {
                                LineSeq::try_new(piece)
                                    .expect("antimeridian split pieces are lineal")
                            })
                            .collect(),
                    ))
                }
            },
            Self::MultiLineString(lines) => {
                let mut split = Vec::with_capacity(lines.len());
                let mut any = false;
                for line in lines {
                    let pieces = segment_coords(&collect_points(line));
                    if pieces.is_empty() {
                        split.push(line.clone());
                    } else {
                        any = true;
                        split.extend(pieces.into_iter().map(CoordSeq::from).map(|piece| {
                            LineSeq::try_new(piece).expect("antimeridian split pieces are lineal")
                        }));
                    }
                }
                let _ = any;
                Ok(Self::MultiLineString(split))
            },
            Self::Polygon(polygon) => {
                let polygons = split_polygon(self, polygon, PoleClosure::Auto)?;
                Ok(match <[Polygon; 1]>::try_from(polygons) {
                    Ok([polygon]) => Self::Polygon(polygon),
                    Err(polygons) => Self::MultiPolygon(polygons),
                })
            },
            Self::MultiPolygon(polygons) => {
                let mut split = Vec::with_capacity(polygons.len());
                for polygon in polygons {
                    split.extend(split_polygon(self, polygon, PoleClosure::Auto)?);
                }
                Ok(Self::MultiPolygon(split))
            },
            Self::GeometryCollection(geometries) => Ok(Self::GeometryCollection(
                geometries
                    .iter()
                    .map(Self::split_antimeridian)
                    .collect::<Result<_>>()?,
            )),
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
    /// Contract: each part must genuinely touch the forced pole — the closure
    /// follows the upstream `force_*_pole` seam semantics, which assume the
    /// regular seam topology of a single pole-touching ring (verified across S2
    /// polar cells at every level). Forcing the wrong pole, or a ring that does
    /// not reach the forced pole, is caller error and yields garbage.
    pub fn split_antimeridian_over_pole(&self, north: bool) -> Result<Self> {
        let closure = if north {
            PoleClosure::North
        } else {
            PoleClosure::South
        };
        match self {
            Self::Polygon(polygon) => {
                let polygons = split_polygon(self, polygon, closure)?;
                Ok(match <[Polygon; 1]>::try_from(polygons) {
                    Ok([polygon]) => Self::Polygon(polygon),
                    Err(polygons) => Self::MultiPolygon(polygons),
                })
            },
            Self::MultiPolygon(polygons) => {
                let mut split = Vec::with_capacity(polygons.len());
                for polygon in polygons {
                    split.extend(split_polygon(self, polygon, closure)?);
                }
                Ok(Self::MultiPolygon(split))
            },
            _ => self.split_antimeridian(),
        }
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

/// Split one polygon into its seam-following parts (upstream
/// `fix_polygon_to_list` with silent winding fixes). `preferred` forces the
/// pole-closure direction (caller knows which pole the ring encircles); `Auto`
/// infers it and retries the other direction only if the inference is invalid.
/// Z/M is rejected only when a pole cap actually survives into the output (see
/// [`reject_fabricated_pole_ordinates`]).
fn split_polygon(
    source: &Shape,
    polygon: &Polygon,
    preferred: PoleClosure,
) -> Result<Vec<Polygon>> {
    let polygons = split_polygon_inner(polygon, preferred)?;
    reject_fabricated_pole_ordinates(source, &polygons)?;
    Ok(polygons)
}

fn split_polygon_inner(polygon: &Polygon, preferred: PoleClosure) -> Result<Vec<Polygon>> {
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
        if segment_coords(&collect_points(hole.coords())).is_empty() {
            stationary_holes.push(hole.clone());
        } else {
            crossing_holes.push(hole.clone());
        }
    }
    if !crossing_holes.is_empty() {
        let axes = polygon.shell.coords().axes();
        let shell_only = Polygon::new(polygon.shell.clone(), stationary_holes);
        let shell_parts = split_polygon_inner(&shell_only, preferred)?;
        let mut result = shape_from_polygons(shell_parts, axes);
        for hole in crossing_holes {
            let hole_axes = hole.coords().axes();
            let north = ring_encloses_pole(hole.coords(), true);
            let south = ring_encloses_pole(hole.coords(), false);
            let hole_closure = match (north, south) {
                (true, false) => PoleClosure::North,
                (false, true) => PoleClosure::South,
                _ => PoleClosure::Auto,
            };
            let hole_parts = split_polygon_inner(&Polygon::new(hole, Vec::new()), hole_closure)?;
            result = result.difference(
                &shape_from_polygons(hole_parts, hole_axes),
                Strictness::Strict,
            )?;
        }
        return polygons_from_shape(result);
    }

    let original = collect_points(polygon.shell.coords());
    let mut exterior = original.clone();
    normalize(&mut exterior);
    let seam_canonicalized = exterior
        .iter()
        .zip(&original)
        .any(|(canonical, raw)| canonical.x != raw.x);
    let exterior = dedup_near(exterior);
    let mut segments = segment_coords(&exterior);
    if segments.is_empty() {
        let _ = seam_canonicalized;
        return Ok(vec![polygon.clone()]);
    }
    let mut kept_holes: Vec<&Ring> = Vec::new();
    for hole in polygon.holes.iter() {
        let coords = collect_points(hole.coords());
        let hole_segments = segment_coords(&coords);
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
                segments.extend(segment_coords(&reversed));
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
    if !capped || polygons.iter().all(polygon_is_valid) {
        return Ok(polygons);
    }
    // The automatic closure picked a pole that self-intersects the ring —
    // the case upstream grew `force_*` flags for. Try each direction; a
    // unique valid candidate wins, a tie is genuinely ambiguous, and when
    // nothing is valid the input itself was broken. Never return the known-
    // invalid automatic candidate as a plausible geometry.
    let mut valid = Vec::new();
    for closure in [PoleClosure::North, PoleClosure::South] {
        let (forced, _) = extend_over_poles(segments.clone(), closure)?;
        if let Ok(candidate) = assemble_polygons(forced, &kept_holes)
            && candidate.iter().all(polygon_is_valid)
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

fn shape_from_polygons(mut polygons: Vec<Polygon>, axes: CoordinateAxes) -> Shape {
    match polygons.len() {
        0 => Shape::typed_empty(EmptyKind::Polygon, axes),
        1 => Shape::Polygon(polygons.pop().expect("one polygon")),
        _ => Shape::MultiPolygon(polygons),
    }
}

fn polygons_from_shape(shape: Shape) -> Result<Vec<Polygon>> {
    match shape {
        Shape::Polygon(polygon) => Ok(vec![polygon]),
        Shape::MultiPolygon(polygons) => Ok(polygons),
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
    // A single non-CCW result encloses both poles: represent it as the
    // world rectangle with the ring as a hole (upstream's pole-covering
    // wrap).
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
            Ring::from_trusted_closed(CoordSeq::from(world)),
            vec![Ring::from_trusted_closed(CoordSeq::from(hole))],
        )]);
    }
    let mut polygons: Vec<Polygon> = shells
        .into_iter()
        .map(|shell| {
            Polygon::new(
                Ring::from_trusted_closed(CoordSeq::from(close_ring(shell))),
                Vec::new(),
            )
        })
        .collect();
    // Assign each non-crossing hole to the part that contains it. (The
    // upstream loop accidentally overwrites earlier holes that land in the
    // same part — accumulating them all is the deliberate difference.)
    let candidate_shapes = polygons
        .iter()
        .cloned()
        .map(Shape::Polygon)
        .collect::<Vec<_>>();
    let mut assigned_holes = vec![Vec::<Ring>::new(); polygons.len()];
    for hole in kept_holes {
        let hole_shape = Shape::LineString(LineSeq::from_trusted(hole.coords().clone()));
        let owner = candidate_shapes
            .iter()
            .position(|candidate| candidate.contains(&hole_shape));
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
        polygon.holes = if polygon.holes.is_empty() {
            holes.into()
        } else {
            let mut combined = polygon.holes.to_vec();
            combined.append(&mut holes);
            combined.into()
        };
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
/// latitudes, or forced when the inference is ambiguous (the internal
/// replacement for upstream's public `force_*` flags).
#[derive(Clone, Copy, PartialEq, Eq)]
enum PoleClosure {
    Auto,
    North,
    South,
}

/// Upstream `extend_over_poles`: a seam end with no seam start between it
/// and the pole closes over that pole; implying both poles under `Auto`
/// means the winding was inverted, so the original segments come back
/// reversed (the silent `fix_winding=True` behavior). Returns the segments
/// and whether any pole closure was applied.
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
            // Forced north from the west side: cap and reverse (upstream's
            // `force_north_pole` branch).
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
            // Forced south from the east side: cap and reverse (upstream's
            // `force_south_pole` branch).
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

/// Upstream `build_polygons`, iteratively: pop a segment, find the closest
/// seam start toward the pole on the same side (donut clause included),
/// join and repeat; a segment with no candidate closes on itself.
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
