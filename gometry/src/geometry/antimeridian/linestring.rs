use crate::geographic::{ChartPole, GeographicFrame};
use crate::geometry::ordinates::{SourceArcRequest, source_arc_ordinates};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    ChartMetric, Coordinates, MOrdinate, Point, RootMetricState, SourceArcMetricAccess, ZOrdinate,
    interpolate_subdivision_ordinate, same_point,
};
use crate::work::Unmetered;

pub(super) fn collect_points<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    points.iter_coords().collect()
}

/// Resolve the two exact chart aliases of a quotient seam from the incoming
/// edge. Pole aliases remain interchangeable. No neighborhood is snapped.
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn resolve_seam_aliases(coords: &mut [Point]) {
    let Some(last) = coords.last() else {
        return;
    };
    let mut previous = last.x;
    for point in coords.iter_mut() {
        let source = point.x;
        if point.y.abs() != 90.0 {
            if source == 180.0 && previous == -180.0 {
                point.x = -180.0;
            } else if source == -180.0 && previous == 180.0 {
                point.x = 180.0;
            }
        }
        previous = source;
    }
}

/// Drop exact consecutive duplicates. One-ULP neighbors are distinct source
/// vertices; Z/M differences also retain the source segment boundary.
fn same_coordinate_values(left: Point, right: Point) -> bool {
    left.x.to_bits() == right.x.to_bits()
        && left.y.to_bits() == right.y.to_bits()
        && left.z() == right.z()
        && left.m() == right.m()
}

fn dedup_consecutive(coords: Vec<Point>) -> Vec<Point> {
    let mut result: Vec<Point> = Vec::with_capacity(coords.len());
    for point in coords {
        if let Some(last) = result.last()
            && same_coordinate_values(*last, point)
        {
            continue;
        }
        result.push(point);
    }
    result
}

fn push_distinct_consecutive(coords: &mut Vec<Point>, point: Point) {
    if coords
        .last()
        .is_none_or(|&last| !same_coordinate_values(last, point))
    {
        coords.push(point);
    }
}

/// Walk a coordinate chain and split it at every antimeridian crossing.
/// Returns the seam-separated pieces, or empty when nothing crosses. A
/// closed input ring re-joins its trailing piece onto the leading one so
/// every returned piece starts and ends on the seam.
pub(crate) fn segment_coords_with_metric<M: ChartMetric>(
    points: &[Point],
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> crate::error::Result<Vec<Vec<Point>>> {
    let mut coords = points.to_vec();
    resolve_seam_aliases(&mut coords);
    let coords = dedup_consecutive(coords);
    let mut piece: Vec<Point> = Vec::new();
    let mut pieces: Vec<Vec<Point>> = Vec::new();
    for &[start, end] in coords.array_windows::<2>() {
        piece.push(start);
        split_transit(&mut piece, &mut pieces, start, end, frame, metric_access)?;
    }
    let Some(last) = coords.last().copied() else {
        return Ok(pieces);
    };
    let Some(first_piece) = pieces.first_mut() else {
        return Ok(pieces);
    };
    let first_piece_start = *first_piece
        .first()
        .expect("seam pieces have an opening coordinate");
    if same_point(last, first_piece_start) {
        // A ring: the trailing run continues the leading piece.
        piece.append(first_piece);
        *first_piece = piece;
        return Ok(pieces);
    }
    piece.push(last);
    pieces.push(piece);
    Ok(pieces)
}

/// A closed ring cut at the seam.
pub(crate) enum SeamRing {
    /// The ring transits the seam: pieces that start and end on it.
    Pieces(Vec<Vec<Point>>),
    /// The ring keeps to one side: its closed coordinates with every seam
    /// vertex spelled on that side.
    Whole(Vec<Point>),
}

/// Cut a closed ring at every seam transit.
///
/// A seam vertex lies on both chart sides, so its spelling decides nothing.
/// A run of seam vertices enters on the side of the vertex it came from and
/// leaves on the side of the vertex it goes to. In a transiting ring each run
/// lies in the cut: seam stitching rebuilds its edges on whichever side the
/// interior occupies, which neither neighbour alone determines. A run that
/// returns to its entry latitude on the side it entered from only touches the
/// seam.
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
pub(crate) fn split_ring_at_seam_with_metric<M: ChartMetric>(
    points: &[Point],
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> crate::error::Result<SeamRing> {
    let coords = dedup_consecutive(points.to_vec());
    let Some((_, cycle)) = coords.split_last() else {
        return Ok(SeamRing::Whole(points.to_vec()));
    };
    let Some(start) = cycle.iter().position(|&point| !on_seam(point)) else {
        // Preserve the source seam runs. A ring that spells both seam aliases
        // without intermediate longitudes collapses to this support.
        let pieces = segment_coords_with_metric(points, frame, metric_access)?;
        return Ok(if pieces.is_empty() {
            SeamRing::Whole(points.to_vec())
        } else {
            SeamRing::Pieces(pieces)
        });
    };
    let cycle_len = cycle.len();
    let vertex = |offset: usize| {
        let shifted = start
            .checked_add(offset)
            .expect("one cycle offset fits the addressable coordinate length");
        let index = shifted.checked_sub(cycle_len).unwrap_or(shifted);
        *cycle
            .get(index)
            .expect("the cycle offset resolves to a source vertex")
    };
    let mut whole = Vec::with_capacity(coords.len());
    whole.push(vertex(0));
    let mut pieces: Vec<Vec<Point>> = Vec::new();
    let mut piece = vec![vertex(0)];
    let mut transits = false;
    let mut offset = 1;
    while offset <= cycle_len {
        let current = vertex(offset);
        if !on_seam(current) {
            let current = if is_chart_pole(current, frame)
                && let Some(&previous) = piece.last()
                && on_seam(previous)
            {
                spelled(current, previous.x)
            } else {
                current
            };
            let previous = *piece.last().expect("a piece holds its opening vertex");
            let cut = pieces.len();
            split_transit(
                &mut piece,
                &mut pieces,
                previous,
                current,
                frame,
                metric_access,
            )?;
            transits |= pieces.len() != cut;
            piece.push(current);
            whole.push(current);
            offset = offset
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            continue;
        }
        let run = offset;
        let first = vertex(run);
        let previous_offset = run
            .checked_sub(1)
            .expect("a seam run follows its off-seam opening vertex");
        let previous = vertex(previous_offset);
        let mut last = first;
        while on_seam(vertex(offset)) {
            last = vertex(offset);
            offset = offset
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        let next = vertex(offset);
        let entry = seam_side(previous, first);
        // At a pole, longitude does not select a side of the chart cut.
        let next_is_pole = is_chart_pole(next, frame);
        let exit = if next_is_pole {
            entry
        } else {
            seam_side(next, last)
        };
        transits |= entry != exit;
        whole.extend((run..offset).map(|index| spelled(vertex(index), entry)));
        if entry == exit && (first.y == last.y || next_is_pole) {
            piece.extend((run..offset).map(|index| spelled(vertex(index), entry)));
        } else {
            piece.push(spelled(first, entry));
            pieces.push(std::mem::replace(&mut piece, vec![spelled(last, exit)]));
        }
    }
    if !transits {
        // `whole` runs from the first off-seam vertex back to it.
        whole.pop();
        whole.rotate_right(start);
        whole.push(*whole.first().expect("a closed ring has an opening vertex"));
        return Ok(SeamRing::Whole(whole));
    }
    // The trailing piece ends at the ring's start, where the leading piece
    // begins.
    let leading = pieces
        .first_mut()
        .expect("a seam transit creates an opening piece");
    piece.extend(leading.drain(1..));
    *leading = piece;
    Ok(SeamRing::Pieces(pieces))
}

/// Whether consecutive ring vertices trace an edge along the seam.
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
pub(crate) fn ring_follows_seam(points: &[Point]) -> bool {
    points
        .array_windows::<2>()
        .any(|&[start, end]| on_seam(start) && on_seam(end) && start.y != end.y)
}

/// A seam vertex other than a pole, where every longitude is the same point.
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn on_seam(point: Point) -> bool {
    point.x.abs() == 180.0 && point.y.abs() != 90.0
}

#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn is_chart_pole(point: Point, frame: GeographicFrame) -> bool {
    point.y == frame.chart_pole_latitude(ChartPole::North)
        || point.y == frame.chart_pole_latitude(ChartPole::South)
}

/// The chart side a seam vertex takes beside an off-seam neighbour: the one
/// the short edge between them reaches. A neighbour on the prime meridian is
/// half a turn away on both sides, and the stored spelling breaks that tie.
fn seam_side(neighbour: Point, vertex: Point) -> f64 {
    if neighbour.x > 0.0 {
        180.0
    } else if neighbour.x < 0.0 {
        -180.0
    } else {
        vertex.x
    }
}

const fn spelled(mut point: Point, side: f64) -> Point {
    point.x = side;
    point
}

/// Close `piece` at the seam and open the next one when the chart edge from
/// `start` to `end` transits it.
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn split_transit<M: ChartMetric>(
    piece: &mut Vec<Point>,
    pieces: &mut Vec<Vec<Point>>,
    start: Point,
    end: Point,
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> crate::error::Result<()> {
    let delta = end.x - start.x;
    if delta > 180.0 && delta != 360.0 {
        // Westward exit: close on -180, reopen on 180.
        let (west, east) = crossing_points(start, end, frame, metric_access)?;
        push_distinct_consecutive(piece, west);
        pieces.push(std::mem::replace(piece, vec![east]));
    } else if -delta > 180.0 && -delta != 360.0 {
        // Eastward exit: close on 180, reopen on -180.
        let (west, east) = crossing_points(end, start, frame, metric_access)?;
        push_distinct_consecutive(piece, east);
        pieces.push(std::mem::replace(piece, vec![west]));
    }
    Ok(())
}

/// The seam-point pair for an ordered chart segment. X/Y follow the affine
/// chart crossing; eligible Z/M follow arclength along that retained edge.
fn crossing_points<M: ChartMetric>(
    west: Point,
    east: Point,
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> crate::error::Result<(Point, Point)> {
    let (latitude, parameter, longitude) = crossing_latitude_and_parameter(west, east, frame)?;
    let source = [west, east];
    let request = SourceArcRequest::new(
        frame,
        &source,
        west.axes().union(east.axes()),
        &parameter,
        || Ok(longitude),
    )?;
    let mut root_metric = RootMetricState::default();
    let mut fraction = None;
    let mut work = Unmetered;
    let ordinates = source_arc_ordinates(
        &request,
        || {
            let access = metric_access.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic antimeridian source ordinates require the operation's CRS metric",
                )
            })?;
            access()
        },
        &mut root_metric,
        &mut fraction,
        &mut work,
    )?;
    let make = |lon: f64| {
        Point::new_axes(
            lon,
            latitude,
            ZOrdinate(ordinates.z.value()),
            MOrdinate(ordinates.m.value()),
        )
    };
    Ok((make(-180.0)?, make(180.0)?))
}

/// Return the affine chart crossing parameter for an ordered chart segment.
///
/// The endpoints are canonical chart longitudes, with `west` below `east`.
/// A seam endpoint remains exact; otherwise the rational parameter follows
/// the edge's retained periodic branch.
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn crossing_latitude_and_parameter(
    west: Point,
    east: Point,
    frame: GeographicFrame,
) -> crate::error::Result<(
    f64,
    ExactRational,
    crate::geographic::RetainedChartDisplacement,
)> {
    let connection = frame
        .longitude()
        .chart_connection(west.x, east.x)
        .map_err(|error| {
            crate::geometry::GeometryErrorKind::antimeridian_split_failed(error.to_string())
        })?;
    let longitude = connection.retained();
    if west.x.abs() == 180.0 {
        return Ok((west.y, ExactRational::zero(), longitude));
    }
    if east.x.abs() == 180.0 {
        return Ok((east.y, ExactRational::one(), longitude));
    }
    let axis = frame.longitude();
    let [start, end] = longitude.endpoints();
    let start = ExactDyadic::from_f64(start.get());
    let branch = ExactDyadic::from_f64(axis.period())
        .try_product(&ExactDyadic::from_i64(longitude.periods()))?;
    let lifted_end = ExactDyadic::from_f64(end.get()).try_add(branch)?;
    let delta = ExactDyadic::try_subtract_refs(&lifted_end, &start)?;
    let seam = if delta.cmp(&ExactDyadic::zero()).is_gt() {
        ExactDyadic::from_f64(axis.half_period())
    } else {
        ExactDyadic::from_f64(-axis.half_period())
    };
    let numerator = seam.try_subtract(start)?;
    let parameter = ExactRational::new(numerator, delta)?;
    let fraction = parameter.round()?;
    // `+ 0.0` canonicalizes a negative-zero crossing latitude.
    Ok((
        interpolate_subdivision_ordinate(west.y, east.y, fraction) + 0.0,
        parameter,
        longitude,
    ))
}
