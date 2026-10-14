use super::super::lineage::{LineageCatalog, LineagePolygon, LineageSequence, PolygonWithLineage};
use super::split::ring_is_ccw;
use super::{
    ChartPole, CoordSeq, GeographicFrame, GeometryErrorKind, MOrdinate, Point, PoleClosure,
    Polygon, Result, Ring, Shape, ZOrdinate, same_point,
};
use crate::geometry::antimeridian::publication::restriction_for_edge;

pub(super) type SeamSegmentsWithLineage = Vec<(Vec<Point>, LineageSequence)>;

struct SeamFragment {
    points: Vec<Point>,
    lineage: Option<LineageSequence>,
}

struct KeptHole<'a> {
    ring: &'a Ring,
    lineage: Option<&'a LineageSequence>,
}

#[derive(Clone, Copy)]
struct PoleCap {
    segment: usize,
    start: Point,
    first: Point,
    second: Point,
    reverse: bool,
}

pub(super) fn polygons_from_shape(shape: Shape) -> Result<Vec<Polygon>> {
    match shape {
        Shape::Polygon(polygon) => Ok(vec![polygon]),
        Shape::MultiPolygon(polygons) => Ok(polygons.iter().cloned().collect()),
        Shape::Empty(..) => Ok(Vec::new()),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_) => Err(GeometryErrorKind::antimeridian_split_failed(
            "subtracting crossing holes produced non-areal output",
        )),
    }
}

pub(super) fn assemble_polygons(
    segments: Vec<Vec<Point>>,
    kept_holes: &[&Ring],
) -> Result<Vec<Polygon>> {
    let segments = segments
        .into_iter()
        .map(|points| SeamFragment {
            points,
            lineage: None,
        })
        .collect();
    let kept_holes = kept_holes
        .iter()
        .map(|ring| KeptHole {
            ring,
            lineage: None,
        })
        .collect::<Vec<_>>();
    assemble_fragments(segments, &kept_holes, None)
        .map(|parts| parts.into_iter().map(|(polygon, _)| polygon).collect())
}

pub(super) fn assemble_polygons_with_lineage(
    segments: SeamSegmentsWithLineage,
    kept_holes: &[(Ring, LineageSequence)],
    catalog: &mut LineageCatalog,
) -> Result<Vec<PolygonWithLineage>> {
    let segments = segments
        .into_iter()
        .map(|(points, lineage)| SeamFragment {
            points,
            lineage: Some(lineage),
        })
        .collect();
    let kept_holes = kept_holes
        .iter()
        .map(|(ring, lineage)| KeptHole {
            ring,
            lineage: Some(lineage),
        })
        .collect::<Vec<_>>();
    assemble_fragments(segments, &kept_holes, Some(catalog))?
        .into_iter()
        .map(|(polygon, lineage)| {
            Ok(PolygonWithLineage {
                polygon,
                lineage: lineage.ok_or_else(|| {
                    GeometryErrorKind::Invalid("assembled polygon has no lineage".into())
                })?,
            })
        })
        .collect()
}

/// Assemble seam fragments once, carrying lineage only for captured callers.
fn assemble_fragments(
    segments: Vec<SeamFragment>,
    kept_holes: &[KeptHole<'_>],
    mut catalog: Option<&mut LineageCatalog>,
) -> Result<Vec<(Polygon, Option<LineagePolygon>)>> {
    let shells = build_rings(segments, catalog.as_deref_mut())?;
    if shells.is_empty() {
        return Err(GeometryErrorKind::antimeridian_split_failed(
            "no ring could be stitched along the seam",
        ));
    }

    if shells.len() == 1 && !ring_is_ccw(&shells[0].points) {
        let like = shells[0]
            .points
            .first()
            .copied()
            .unwrap_or(Point::new(0.0, 0.0)?);
        let corner = |lon: f64, lat: f64| {
            Point::new_axes(lon, lat, ZOrdinate(like.z()), MOrdinate(like.m()))
        };
        let world = vec![
            corner(-180.0, 90.0)?,
            corner(-180.0, -90.0)?,
            corner(180.0, -90.0)?,
            corner(180.0, 90.0)?,
            corner(-180.0, 90.0)?,
        ];
        let shell = shells.into_iter().next().expect("one shell is available");
        let shell = close_fragment(shell, catalog.as_deref_mut())?;
        let holes = vec![Ring::try_from_closed(CoordSeq::try_from_points(
            &shell.points,
        )?)?];
        let lineage = match (shell.lineage, catalog.as_deref_mut()) {
            (Some(lineage), Some(catalog)) => {
                let world_lineage = synthetic_lineage_ring(lineage.member, &world, catalog)?;
                Some(LineagePolygon {
                    member: lineage.member,
                    shell: world_lineage,
                    holes: vec![lineage],
                })
            },
            (None, None) => None,
            _ => {
                return Err(GeometryErrorKind::Invalid(
                    "assembled shell lineage and catalog disagree".into(),
                )
                .into());
            },
        };
        let polygon = Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&world)?)?,
            holes,
        );
        return Ok(vec![(polygon, lineage)]);
    }

    let mut candidates = Vec::with_capacity(shells.len());
    let mut polygons = Vec::with_capacity(shells.len());
    let mut shell_lineages = Vec::with_capacity(shells.len());
    for shell in shells {
        let shell = close_fragment(shell, catalog.as_deref_mut())?;
        let ring = Ring::try_from_closed(CoordSeq::try_from_points(&shell.points)?)?;
        candidates.push(Shape::Polygon(Polygon::new(ring.clone(), Vec::new())));
        polygons.push(Polygon::new(ring, Vec::new()));
        shell_lineages.push(shell.lineage);
    }

    let mut assigned = (0..polygons.len())
        .map(|_| Vec::<(Ring, Option<LineageSequence>)>::new())
        .collect::<Vec<_>>();
    for hole in kept_holes {
        let hole_shape = Shape::LineString(hole.ring.as_line());
        let mut owner = None;
        for (candidate, assigned_holes) in candidates.iter().zip(&mut assigned) {
            if candidate.contains(&hole_shape)? {
                owner = Some(assigned_holes);
                break;
            }
        }
        match owner {
            Some(assigned_holes) => {
                assigned_holes.push((hole.ring.clone(), hole.lineage.cloned()));
            },
            None => {
                return Err(GeometryErrorKind::antimeridian_split_failed(
                    "a hole does not lie inside any split part",
                ));
            },
        }
    }

    let mut output = Vec::with_capacity(polygons.len());
    for ((polygon, shell_lineage), holes) in polygons.into_iter().zip(shell_lineages).zip(assigned)
    {
        let mut hole_rings = Vec::with_capacity(holes.len());
        let mut hole_lineages = Vec::with_capacity(holes.len());
        for (ring, lineage) in holes {
            hole_rings.push(ring);
            if let Some(lineage) = lineage {
                hole_lineages.push(lineage);
            }
        }
        let lineage = match (shell_lineage, catalog.is_some()) {
            (Some(shell), true) if hole_lineages.len() == hole_rings.len() => {
                Some(LineagePolygon {
                    member: shell.member,
                    shell,
                    holes: hole_lineages,
                })
            },
            (None, false) => None,
            _ => {
                return Err(GeometryErrorKind::Invalid(
                    "assembled polygon rings and lineage disagree".into(),
                )
                .into());
            },
        };
        output.push((Polygon::new(polygon.shell, hole_rings), lineage));
    }
    Ok(output)
}

#[expect(clippy::float_cmp, reason = "seam banks use exact ±180 chart literals")]
fn build_rings(
    mut segments: Vec<SeamFragment>,
    mut catalog: Option<&mut LineageCatalog>,
) -> Result<Vec<SeamFragment>> {
    let mut rings = Vec::new();
    while let Some(mut segment) = segments.pop() {
        let end = *segment.points.last().expect("segments are non-empty");
        let start = *segment.points.first().expect("segments are non-empty");
        let is_right = end.x == 180.0;
        let mut best: Option<(usize, f64)> = None;
        let self_candidate = is_self_closing(&segment.points).then_some(start.y);
        for (index, candidate) in segments.iter().enumerate() {
            let candidate_start = *candidate.points.first().expect("segments are non-empty");
            let candidate_end = *candidate.points.last().expect("segments are non-empty");
            if candidate_start.x != end.x {
                continue;
            }
            let toward_pole = if is_right {
                candidate_start.y > end.y
                    && (!is_self_closing(&candidate.points) || candidate_end.y < start.y)
            } else {
                candidate_start.y < end.y
                    && (!is_self_closing(&candidate.points) || candidate_end.y > start.y)
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
            let mut joined = segments.remove(index);
            match (segment.lineage.as_mut(), joined.lineage.take()) {
                (Some(lineage), Some(joined_lineage)) => {
                    let catalog = catalog.as_deref_mut().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "captured seam fragments have no lineage catalog".into(),
                        )
                    })?;
                    let restriction = restriction_for_edge(
                        *segment.points.last().expect("segments are non-empty"),
                        *joined.points.first().expect("segments are non-empty"),
                    );
                    let edge = catalog.push_edge_use(lineage.member, Vec::new(), restriction)?;
                    lineage.edges.push(edge);
                    lineage.edges.extend(joined_lineage.edges);
                    lineage.vertices.extend(joined_lineage.vertices);
                },
                (None, None) => {},
                _ => {
                    return Err(GeometryErrorKind::Invalid(
                        "seam fragment geometry and lineage disagree".into(),
                    )
                    .into());
                },
            }
            segment.points.append(&mut joined.points);
            segments.push(segment);
            continue;
        }
        if !segment.points.iter().all(|point| same_point(*point, start)) {
            rings.push(segment);
        }
    }
    Ok(rings)
}

fn close_fragment(
    mut fragment: SeamFragment,
    mut catalog: Option<&mut LineageCatalog>,
) -> Result<SeamFragment> {
    if let (Some(first), Some(last)) = (
        fragment.points.first().copied(),
        fragment.points.last().copied(),
    ) && !same_point(first, last)
    {
        match fragment.lineage.as_mut() {
            Some(lineage) => {
                let catalog = catalog.as_deref_mut().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "captured seam fragment has no lineage catalog".into(),
                    )
                })?;
                let edge = catalog.push_edge_use(
                    lineage.member,
                    Vec::new(),
                    restriction_for_edge(last, first),
                )?;
                lineage.edges.push(edge);
                let first_events = lineage.vertices.first().cloned().ok_or_else(|| {
                    GeometryErrorKind::Invalid("ring lineage has no first vertex".into())
                })?;
                lineage.vertices.push(first_events);
            },
            None if catalog.is_none() => {},
            None => {
                return Err(GeometryErrorKind::Invalid(
                    "seam fragment geometry and lineage disagree".into(),
                )
                .into());
            },
        }
        fragment.points.push(first);
    }
    Ok(fragment)
}

fn synthetic_lineage_ring(
    member: super::super::lineage::MemberId,
    points: &[Point],
    catalog: &mut LineageCatalog,
) -> Result<LineageSequence> {
    let mut vertices = Vec::with_capacity(points.len());
    for _ in points {
        let event = catalog.event(member, Vec::new())?;
        vertices.push(vec![event]);
    }
    let mut edges = Vec::with_capacity(points.len().saturating_sub(1));
    for &[start, end] in points.array_windows::<2>() {
        edges.push(catalog.push_edge_use(member, Vec::new(), restriction_for_edge(start, end))?);
    }
    Ok(LineageSequence {
        member,
        vertices,
        edges,
    })
}

/// A seam end with no seam start between it and the pole closes over that pole.
/// Implying both poles under `Auto` means the winding was inverted, so the
/// original segments come back reversed. Returns the segments and whether any
/// pole closure was applied.
pub(super) fn extend_over_poles(
    segments: Vec<Vec<Point>>,
    closure: PoleClosure,
) -> Result<(Vec<Vec<Point>>, bool)> {
    let (segments, caps, _) = extend_over_poles_with_direction(segments, closure)?;
    Ok((segments, !caps.is_empty()))
}

pub(super) fn extend_over_poles_with_lineage(
    segments: SeamSegmentsWithLineage,
    closure: PoleClosure,
    catalog: &mut LineageCatalog,
) -> Result<(SeamSegmentsWithLineage, bool)> {
    let plain_segments = segments.iter().map(|(points, _)| points.clone()).collect();
    let (stitched, caps, reversed) = extend_over_poles_with_direction(plain_segments, closure)?;
    if stitched.len() != segments.len() {
        return Err(GeometryErrorKind::Invalid(
            "pole extension changed the seam fragment count".into(),
        )
        .into());
    }
    let mut result = Vec::with_capacity(segments.len());
    for ((_, lineage), points) in segments.into_iter().zip(stitched) {
        let lineage = if reversed {
            catalog.reverse_sequence(&lineage)?
        } else {
            lineage
        };
        result.push((points, lineage));
    }
    let capped = !caps.is_empty();
    for cap in caps {
        let (_, lineage) = result.get_mut(cap.segment).ok_or_else(|| {
            GeometryErrorKind::Invalid("pole cap references an unknown seam fragment".into())
        })?;
        let member = lineage.member;
        let first_event = catalog.event(member, Vec::new())?;
        let second_event = catalog.event(member, Vec::new())?;
        if cap.reverse {
            let reversed = catalog.reverse_sequence(lineage)?;
            let mut cap_vertices = vec![vec![second_event], vec![first_event]];
            cap_vertices.extend(reversed.vertices);
            let mut cap_edges = vec![
                catalog.push_edge_use(
                    member,
                    Vec::new(),
                    restriction_for_edge(cap.second, cap.first),
                )?,
                catalog.push_edge_use(
                    member,
                    Vec::new(),
                    restriction_for_edge(cap.first, cap.start),
                )?,
            ];
            cap_edges.extend(reversed.edges);
            *lineage = LineageSequence {
                member,
                vertices: cap_vertices,
                edges: cap_edges,
            };
        } else {
            lineage
                .vertices
                .extend([vec![first_event], vec![second_event]]);
            lineage.edges.extend([
                catalog.push_edge_use(
                    member,
                    Vec::new(),
                    restriction_for_edge(cap.start, cap.first),
                )?,
                catalog.push_edge_use(
                    member,
                    Vec::new(),
                    restriction_for_edge(cap.first, cap.second),
                )?,
            ]);
        }
    }
    Ok((result, capped))
}

#[expect(
    clippy::float_cmp,
    reason = "pole closure classifies exact seam and pole chart literals"
)]
fn extend_over_poles_with_direction(
    mut segments: Vec<Vec<Point>>,
    closure: PoleClosure,
) -> Result<(Vec<Vec<Point>>, Vec<PoleCap>, bool)> {
    let chart_frame = GeographicFrame::crs84();
    let north_pole = chart_frame.chart_pole_latitude(ChartPole::North);
    let south_pole = chart_frame.chart_pole_latitude(ChartPole::South);
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
        let start = *segment.first().expect("segments are non-empty");
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
    let mut caps = Vec::new();
    let mut over_north = false;
    let mut over_south = false;
    if let Some((index, latitude)) = left_end {
        if closure == PoleClosure::Chart(ChartPole::North)
            && right_end.is_none()
            && left_start.is_none_or(|start| latitude > start)
        {
            // Forced north from the west side: cap and reverse.
            over_north = true;
            let segment = segments
                .get_mut(index)
                .expect("the pole endpoint came from this segment");
            let like = *segment.last().expect("segments are non-empty");
            let first = pole_vertex(like, -180.0, north_pole)?;
            let second = pole_vertex(like, 180.0, north_pole)?;
            segment.push(first);
            segment.push(second);
            segment.reverse();
            caps.push(PoleCap {
                segment: index,
                start: like,
                first,
                second,
                reverse: true,
            });
        } else if closure == PoleClosure::Chart(ChartPole::South)
            || left_start.is_none_or(|start| latitude < start)
        {
            over_south = true;
            let segment = segments
                .get_mut(index)
                .expect("the pole endpoint came from this segment");
            let like = *segment.last().expect("segments are non-empty");
            let first = pole_vertex(like, -180.0, south_pole)?;
            let second = pole_vertex(like, 180.0, south_pole)?;
            segment.push(first);
            segment.push(second);
            caps.push(PoleCap {
                segment: index,
                start: like,
                first,
                second,
                reverse: false,
            });
        }
    }
    if let Some((index, latitude)) = right_end {
        if closure == PoleClosure::Chart(ChartPole::South)
            && right_start.is_none_or(|start| latitude < start)
        {
            // Forced south from the east side: cap and reverse.
            over_south = true;
            let segment = segments
                .get_mut(index)
                .expect("the pole endpoint came from this segment");
            let like = *segment.last().expect("segments are non-empty");
            let first = pole_vertex(like, 180.0, south_pole)?;
            let second = pole_vertex(like, -180.0, south_pole)?;
            segment.push(first);
            segment.push(second);
            segment.reverse();
            caps.push(PoleCap {
                segment: index,
                start: like,
                first,
                second,
                reverse: true,
            });
        } else if closure == PoleClosure::Chart(ChartPole::North)
            || right_start.is_none_or(|start| latitude > start)
        {
            over_north = true;
            let segment = segments
                .get_mut(index)
                .expect("the pole endpoint came from this segment");
            let like = *segment.last().expect("segments are non-empty");
            let first = pole_vertex(like, 180.0, north_pole)?;
            let second = pole_vertex(like, -180.0, north_pole)?;
            segment.push(first);
            segment.push(second);
            caps.push(PoleCap {
                segment: index,
                start: like,
                first,
                second,
                reverse: false,
            });
        }
    }
    if closure == PoleClosure::Auto && over_north && over_south {
        // Both poles implied: the input ring was wound backwards. Reverse
        // every original segment instead.
        let mut reversed = original;
        for segment in &mut reversed {
            segment.reverse();
        }
        return Ok((reversed, Vec::new(), true));
    }
    Ok((segments, caps, false))
}

#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn is_self_closing(segment: &[Point]) -> bool {
    let start = *segment.first().expect("segments are non-empty");
    let end = *segment.last().expect("segments are non-empty");
    let is_right = end.x == 180.0;
    start.x == end.x && ((is_right && start.y > end.y) || (!is_right && start.y < end.y))
}
