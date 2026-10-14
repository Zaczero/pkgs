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

/// Drop exact consecutive duplicates. One-ULP neighbors are distinct source
/// vertices; Z/M differences also retain the source segment boundary.
fn same_coordinate_values(left: Point, right: Point) -> bool {
    left.x.to_bits() == right.x.to_bits()
        && left.y.to_bits() == right.y.to_bits()
        && left.z() == right.z()
        && left.m() == right.m()
}

trait TransitVertex: Clone + Sized {
    fn point(&self) -> Point;
    fn set_point(&mut self, point: Point);
    fn edge_index(&self, fallback: usize) -> usize;
    fn merge_duplicate(&mut self, duplicate: Self);
    fn edge_pair(west: Point, east: Point, edge: usize, parameter: ExactRational) -> (Self, Self);
    fn push_final(output: &mut Vec<Self>, point: Self);
}

impl TransitVertex for Point {
    fn point(&self) -> Point {
        *self
    }

    fn set_point(&mut self, point: Point) {
        *self = point;
    }

    fn edge_index(&self, fallback: usize) -> usize {
        fallback
    }

    fn merge_duplicate(&mut self, _: Self) {}

    fn edge_pair(west: Point, east: Point, _: usize, _: ExactRational) -> (Self, Self) {
        (west, east)
    }

    fn push_final(output: &mut Vec<Self>, point: Self) {
        output.push(point);
    }
}

/// Resolve the two exact chart aliases of a quotient seam from the incoming
/// edge. Pole aliases remain interchangeable. No neighborhood is snapped.
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn resolve_seam_aliases<P: TransitVertex>(coords: &mut [P]) {
    let Some(last) = coords.last() else {
        return;
    };
    let mut previous = last.point().x;
    for vertex in coords {
        let mut point = vertex.point();
        let source = point.x;
        if point.y.abs() != 90.0 {
            if source == 180.0 && previous == -180.0 {
                point.x = -180.0;
            } else if source == -180.0 && previous == 180.0 {
                point.x = 180.0;
            }
        }
        vertex.set_point(point);
        previous = source;
    }
}

fn push_distinct<P: TransitVertex>(coords: &mut Vec<P>, point: P) {
    if let Some(last) = coords.last_mut()
        && same_coordinate_values(last.point(), point.point())
    {
        last.merge_duplicate(point);
    } else {
        coords.push(point);
    }
}

fn dedup_consecutive<P: TransitVertex>(coords: Vec<P>) -> Vec<P> {
    let mut result = Vec::with_capacity(coords.len());
    for point in coords {
        push_distinct(&mut result, point);
    }
    result
}

/// Walk one chart-coordinate sequence, retaining source occurrences only for
/// callers that need to compose lineage.
fn segment_coords_core<P: TransitVertex, M: ChartMetric>(
    mut coords: Vec<P>,
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> crate::error::Result<Vec<Vec<P>>> {
    resolve_seam_aliases(&mut coords);
    let coords = dedup_consecutive(coords);
    let mut piece = Vec::new();
    let mut pieces = Vec::new();
    for (edge, [start, end]) in coords.array_windows::<2>().enumerate() {
        push_distinct(&mut piece, start.clone());
        split_transit(
            &mut piece,
            &mut pieces,
            start.point(),
            end.point(),
            start.edge_index(edge),
            frame,
            metric_access,
        )?;
    }
    let Some(last) = coords.last().cloned() else {
        return Ok(pieces);
    };
    let Some(first_piece) = pieces.first_mut() else {
        return Ok(pieces);
    };
    let first_piece_start = first_piece
        .first()
        .expect("seam pieces have an opening coordinate")
        .point();
    if same_point(last.point(), first_piece_start) {
        // A ring: the trailing run continues the leading piece.
        piece.append(first_piece);
        *first_piece = piece;
        return Ok(pieces);
    }
    P::push_final(&mut piece, last);
    pieces.push(piece);
    Ok(pieces)
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
    segment_coords_core(points.to_vec(), frame, metric_access)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SeamVertexSource {
    InputVertex(usize),
    InputEdge {
        edge: usize,
        parameter: ExactRational,
    },
}

#[derive(Clone)]
struct IndexedPoint {
    point: Point,
    source_index: usize,
    sources: Vec<SeamVertexSource>,
}

impl TransitVertex for IndexedPoint {
    fn point(&self) -> Point {
        self.point
    }

    fn set_point(&mut self, point: Point) {
        self.point = point;
    }

    fn edge_index(&self, _: usize) -> usize {
        self.source_index
    }

    fn merge_duplicate(&mut self, mut duplicate: Self) {
        self.sources.append(&mut duplicate.sources);
        self.source_index = duplicate.source_index;
    }

    fn edge_pair(west: Point, east: Point, edge: usize, parameter: ExactRational) -> (Self, Self) {
        let source = |point, parameter| Self {
            point,
            source_index: edge,
            sources: vec![SeamVertexSource::InputEdge { edge, parameter }],
        };
        (source(west, parameter.clone()), source(east, parameter))
    }

    fn push_final(output: &mut Vec<Self>, point: Self) {
        push_distinct(output, point);
    }
}

fn indexed_points(points: &[Point]) -> Vec<IndexedPoint> {
    let mut indexed: Vec<IndexedPoint> = Vec::with_capacity(points.len());
    for (source_index, point) in points.iter().copied().enumerate() {
        let source = SeamVertexSource::InputVertex(source_index);
        if let Some(previous) = indexed.last_mut()
            && same_coordinate_values(previous.point, point)
        {
            previous.source_index = source_index;
            previous.sources.push(source);
        } else {
            indexed.push(IndexedPoint {
                point,
                source_index,
                sources: vec![source],
            });
        }
    }
    if indexed.len() > 1
        && same_coordinate_values(
            indexed
                .first()
                .expect("a ring has an opening coordinate")
                .point,
            indexed
                .last()
                .expect("a ring has a closing coordinate")
                .point,
        )
    {
        let closing_sources = indexed
            .last()
            .expect("a ring has a closing coordinate")
            .sources
            .clone();
        let opening = indexed
            .first_mut()
            .expect("a ring has an opening coordinate");
        for source in closing_sources {
            if !opening.sources.contains(&source) {
                opening.sources.push(source);
            }
        }
    }
    indexed
}

const fn spelled_indexed(mut point: IndexedPoint, side: f64) -> IndexedPoint {
    point.point.x = side;
    point
}

fn merge_closed_occurrences(points: &mut [IndexedPoint]) {
    let (Some(first), Some(last)) = (points.first(), points.last()) else {
        return;
    };
    let mut sources = first.sources.clone();
    for source in &last.sources {
        if !sources.contains(source) {
            sources.push(source.clone());
        }
    }
    if let Some(first) = points.first_mut() {
        first.sources.clone_from(&sources);
    }
    if let Some(last) = points.last_mut() {
        last.sources = sources;
    }
}

fn seam_piece(points: Vec<IndexedPoint>) -> SeamPiece {
    let (points, vertices) = points
        .into_iter()
        .map(|point| (point.point, point.sources))
        .unzip();
    SeamPiece { points, vertices }
}

#[derive(Clone)]
pub(crate) struct SeamPiece {
    pub(crate) points: Vec<Point>,
    pub(crate) vertices: Vec<Vec<SeamVertexSource>>,
}

/// A closed ring cut at the seam, retaining each output vertex's input use.
pub(crate) enum SeamRing {
    /// The ring transits the seam: pieces that start and end on it.
    Pieces(Vec<SeamPiece>),
    /// The ring keeps to one side: its closed coordinates with every seam
    /// vertex spelled on that side.
    Whole(SeamPiece),
}

pub(in crate::geometry) fn segment_coords_with_occurrences<M: ChartMetric>(
    points: &[Point],
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> crate::error::Result<Vec<SeamPiece>> {
    segment_coords_core(indexed_points(points), frame, metric_access)
        .map(|pieces| pieces.into_iter().map(seam_piece).collect())
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
    let coords = indexed_points(points);
    let Some((_, cycle)) = coords.split_last() else {
        return Ok(SeamRing::Whole(seam_piece(coords)));
    };
    let Some(start) = cycle.iter().position(|vertex| !on_seam(vertex.point)) else {
        // Preserve the source seam runs. A ring that spells both seam aliases
        // without intermediate longitudes collapses to this support.
        let pieces = segment_coords_with_occurrences(points, frame, metric_access)?;
        return Ok(if pieces.is_empty() {
            SeamRing::Whole(seam_piece(coords))
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
        cycle
            .get(index)
            .expect("the cycle offset resolves to a source vertex")
            .clone()
    };
    let mut whole = Vec::with_capacity(coords.len());
    whole.push(vertex(0));
    let mut pieces: Vec<Vec<IndexedPoint>> = Vec::new();
    let mut piece = vec![vertex(0)];
    let mut transits = false;
    let mut offset = 1;
    while offset <= cycle_len {
        let current = vertex(offset);
        if !on_seam(current.point) {
            let current = if is_chart_pole(current.point, frame)
                && let Some(previous) = piece.last()
                && on_seam(previous.point)
            {
                spelled_indexed(current, previous.point.x)
            } else {
                current
            };
            let previous = piece
                .last()
                .expect("a piece holds its opening vertex")
                .point;
            let edge = piece
                .last()
                .expect("a piece holds its opening vertex")
                .source_index;
            let cut = pieces.len();
            split_transit(
                &mut piece,
                &mut pieces,
                previous,
                current.point,
                edge,
                frame,
                metric_access,
            )?;
            transits |= pieces.len() != cut;
            push_distinct(&mut piece, current.clone());
            push_distinct(&mut whole, current);
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
        let mut last = first.clone();
        while on_seam(vertex(offset).point) {
            last = vertex(offset);
            offset = offset
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        let next = vertex(offset);
        let entry = seam_side(previous.point, first.point);
        // At a pole, longitude does not select a side of the chart cut.
        let next_is_pole = is_chart_pole(next.point, frame);
        let exit = if next_is_pole {
            entry
        } else {
            seam_side(next.point, last.point)
        };
        transits |= entry != exit;
        for index in run..offset {
            push_distinct(&mut whole, spelled_indexed(vertex(index), entry));
        }
        if entry == exit && (first.point.y == last.point.y || next_is_pole) {
            for index in run..offset {
                push_distinct(&mut piece, spelled_indexed(vertex(index), entry));
            }
        } else {
            push_distinct(&mut piece, spelled_indexed(first, entry));
            pieces.push(std::mem::replace(&mut piece, vec![spelled_indexed(
                last, exit,
            )]));
        }
    }
    if !transits {
        // `whole` runs from the first off-seam vertex back to it.
        whole.pop();
        whole.rotate_right(start);
        whole.push(
            whole
                .first()
                .expect("a closed ring has an opening vertex")
                .clone(),
        );
        merge_closed_occurrences(&mut whole);
        return Ok(SeamRing::Whole(seam_piece(whole)));
    }
    // The trailing piece ends at the ring's start, where the leading piece
    // begins.
    let leading = pieces
        .first_mut()
        .expect("a seam transit creates an opening piece");
    piece.extend(leading.drain(1..));
    *leading = piece;
    Ok(SeamRing::Pieces(
        pieces.into_iter().map(seam_piece).collect(),
    ))
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

/// Close `piece` at the seam and open the next one when the chart edge from
/// `start` to `end` transits it.
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact seam transits and full-turn chart edges"
)]
fn split_transit<P: TransitVertex, M: ChartMetric>(
    piece: &mut Vec<P>,
    pieces: &mut Vec<Vec<P>>,
    start: Point,
    end: Point,
    edge: usize,
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> crate::error::Result<()> {
    let delta = end.x - start.x;
    if delta > 180.0 && delta != 360.0 {
        // Westward exit: close on -180, reopen on 180.
        let (west, east, parameter) = crossing_points(start, end, frame, metric_access)?;
        let (west, east) = P::edge_pair(west, east, edge, parameter);
        push_distinct(piece, west);
        pieces.push(std::mem::replace(piece, vec![east]));
    } else if -delta > 180.0 && -delta != 360.0 {
        // Eastward exit: close on 180, reopen on -180.
        let (west, east, reverse_parameter) = crossing_points(end, start, frame, metric_access)?;
        let mut work = Unmetered;
        let parameter =
            ExactRational::one().try_subtract_with_work(&reverse_parameter, &mut work)?;
        let (west, east) = P::edge_pair(west, east, edge, parameter);
        push_distinct(piece, east);
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
) -> crate::error::Result<(Point, Point, ExactRational)> {
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
    Ok((make(-180.0)?, make(180.0)?, parameter))
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
