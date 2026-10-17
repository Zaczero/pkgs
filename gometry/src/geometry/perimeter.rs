use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

use super::area::lifted_ring_area_sign;
use super::storage::PackedRow;
use super::storage::packed_columns::PolygonColumns;
use super::{Bounds, ChartMetric, CoordSeq, Point, Polygon, Ring, Shape, XY, canonicalize_zero};
use crate::crs::ChartEdge;
use crate::error::Result;
use crate::geographic::lonlat::geographic_admission_error;
use crate::geographic::{GeographicFrame, GeographicLocation, Latitude, SeamIdentity, ring_gate};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};

const SHELL_ROLE: i8 = 1;
const HOLE_ROLE: i8 = -1;

pub(crate) fn geographic_polygonal_perimeter(
    metric: &impl ChartMetric,
    shape: &Shape,
    frame: GeographicFrame,
    cached_bounds: Option<Bounds>,
) -> Result<f64> {
    geographic_perimeter(
        metric,
        &PerimeterOperand::Shape(shape),
        frame,
        cached_bounds,
    )
}

pub(crate) fn geographic_packed_polygonal_perimeter(
    metric: &impl ChartMetric,
    columns: &PolygonColumns<'_>,
    logical_row: usize,
    frame: GeographicFrame,
    cached_bounds: Option<Bounds>,
) -> Result<f64> {
    let row = columns.packed_row(logical_row);
    let operand = PerimeterOperand::Packed {
        coords: columns.coords(),
        row,
    };
    if operand.ring_count() == 0 {
        return Ok(0.0);
    }
    if perimeter_screen_passes(&operand, frame, cached_bounds) {
        return literal_perimeter(metric, &operand);
    }
    geographic_perimeter_slow(metric, &operand, frame, true)
}

fn geographic_perimeter(
    metric: &impl ChartMetric,
    operand: &PerimeterOperand<'_>,
    frame: GeographicFrame,
    cached_bounds: Option<Bounds>,
) -> Result<f64> {
    if operand.ring_count() == 0 {
        return Ok(0.0);
    }
    if perimeter_screen_passes(operand, frame, cached_bounds) {
        return literal_perimeter(metric, operand);
    }
    geographic_perimeter_slow(metric, operand, frame, false)
}

fn geographic_perimeter_slow(
    metric: &impl ChartMetric,
    operand: &PerimeterOperand<'_>,
    frame: GeographicFrame,
    check_full_turn_gate: bool,
) -> Result<f64> {
    let mut perimeter = 0.0;
    let mut correction = 0.0;
    let mut seam_pieces = Vec::new();
    let mut ring_data = Vec::with_capacity(operand.ring_count());
    let mut ring_index = 0_usize;
    operand.for_each_ring(&mut |ring| {
        scan_ring(
            metric,
            frame,
            ring_index,
            &ring,
            &mut seam_pieces,
            &mut perimeter,
            &mut correction,
        )?;
        ring_data.push(RingData {
            source: ring,
            orientation: None,
            lifted_extent_at_least_period: false,
        });
        ring_index = ring_index
            .checked_add(1)
            .expect("ring index fits its collection");
        Ok(())
    })?;
    let mut needs_orientation = vec![false; ring_data.len()];
    let seam_arcs = seam_pieces
        .iter()
        .map(|piece| ArcPiece {
            lower: piece.lower.clone(),
            upper: piece.upper.clone(),
            direction: piece.direction,
            lift: 0,
            source: piece.ring,
        })
        .collect::<Vec<_>>();
    sweep_pieces(&seam_arcs, |_, _, active| {
        if let Some(&first) = active.first()
            && active.iter().any(|&index| {
                seam_arcs.get(index).expect("active seam arc exists").source
                    != seam_arcs.get(first).expect("active seam arc exists").source
            })
        {
            for &index in active {
                let source = seam_arcs.get(index).expect("active seam arc exists").source;
                *needs_orientation
                    .get_mut(source)
                    .expect("seam source ring exists") = true;
            }
        }
        Ok(())
    })?;
    for (ring_index, (ring, &needs_orientation)) in
        ring_data.iter_mut().zip(&needs_orientation).enumerate()
    {
        let area =
            lifted_ring_area_sign(frame, ring.source.xs(), ring.source.ys(), needs_orientation)?;
        ring.orientation = area.orientation;
        ring.lifted_extent_at_least_period = area.full_extent;
        if check_full_turn_gate {
            let first = ring.source.first_point();
            ring_gate(
                area.chart_turn,
                area.south,
                area.north,
                GeographicLocation {
                    ring: Some(ring_index),
                    coordinate: 0,
                    ..GeographicLocation::default()
                },
                XY::new(first.x, first.y),
            )
            .map_err(|error| geographic_admission_error(&error))?;
        }
    }

    let (mut groups, sources) = in_ring_arc_groups(metric, frame, &ring_data)?;
    sort_arc_groups(&mut groups)?;
    for group in &groups {
        cancel_in_ring_group(metric, group, &sources, &mut perimeter, &mut correction)?;
    }
    cancel_seam_pieces(
        metric,
        frame,
        &mut ring_data,
        &seam_pieces,
        &mut perimeter,
        &mut correction,
    )?;
    Ok(canonicalize_zero(perimeter))
}

enum PerimeterOperand<'a> {
    Shape(&'a Shape),
    Packed {
        coords: &'a CoordSeq,
        row: PackedRow<'a>,
    },
}

impl<'a> PerimeterOperand<'a> {
    fn ring_count(&self) -> usize {
        match self {
            Self::Shape(Shape::Polygon(polygon)) => polygon.holes.len().saturating_add(1),
            Self::Shape(Shape::MultiPolygon(polygons)) => {
                polygons.iter().fold(0_usize, |count, polygon| {
                    count.saturating_add(polygon.holes.len()).saturating_add(1)
                })
            },
            Self::Shape(_) => 0,
            Self::Packed { row, .. } => row
                .polygon_rings()
                .expect("packed perimeter source contains polygon rings")
                .len(),
        }
    }

    fn for_each_ring(&self, visit: &mut impl FnMut(PerimeterRing<'a>) -> Result<()>) -> Result<()> {
        match self {
            Self::Shape(Shape::Polygon(polygon)) => visit_polygon_rings(polygon, visit),
            Self::Shape(Shape::MultiPolygon(polygons)) => {
                for polygon in polygons.iter() {
                    visit_polygon_rings(polygon, visit)?;
                }
                Ok(())
            },
            Self::Shape(_) => Ok(()),
            Self::Packed { coords, row } => {
                let rings = row
                    .polygon_rings()
                    .expect("packed perimeter source contains polygon rings");
                for (ordinal, index) in rings.enumerate() {
                    let window = row
                        .polygon_ring_window(index)
                        .expect("packed row ring window is admitted");
                    visit(PerimeterRing {
                        coords,
                        window,
                        role: if ordinal == 0 { SHELL_ROLE } else { HOLE_ROLE },
                    })?;
                }
                Ok(())
            },
        }
    }

    fn longitude_bounds(&self) -> Option<(f64, f64)> {
        let mut bounds = LongitudeBounds::default();
        self.for_each_ring(&mut |ring| {
            bounds.fold(ring.xs());
            Ok(())
        })
        .expect("longitude folding cannot fail");
        match (bounds.minimum, bounds.maximum) {
            (Some(minimum), Some(maximum)) => Some((minimum, maximum)),
            _ => None,
        }
    }
}

#[derive(Clone)]
struct PerimeterRing<'a> {
    coords: &'a CoordSeq,
    window: std::ops::Range<usize>,
    role: i8,
}

impl<'a> PerimeterRing<'a> {
    fn from_ring(ring: &'a Ring, role: i8) -> Self {
        Self {
            coords: ring.coords(),
            window: 0..ring.len(),
            role,
        }
    }

    fn first_point(&self) -> Point {
        let (&x, &y) = self
            .xs()
            .first()
            .zip(self.ys().first())
            .expect("an admitted perimeter ring has a first coordinate");
        Point::new_raw_xy(x, y)
    }

    fn xs(&self) -> &'a [f64] {
        self.coords
            .xs()
            .get(self.window.clone())
            .expect("perimeter ring window is admitted")
    }

    fn ys(&self) -> &'a [f64] {
        self.coords
            .ys()
            .get(self.window.clone())
            .expect("perimeter ring window is admitted")
    }
}

fn visit_polygon_rings<'a>(
    polygon: &'a Polygon,
    visit: &mut impl FnMut(PerimeterRing<'a>) -> Result<()>,
) -> Result<()> {
    visit(PerimeterRing::from_ring(&polygon.shell, SHELL_ROLE))?;
    for ring in polygon.holes.iter() {
        visit(PerimeterRing::from_ring(ring, HOLE_ROLE))?;
    }
    Ok(())
}

fn literal_perimeter(metric: &impl ChartMetric, operand: &PerimeterOperand<'_>) -> Result<f64> {
    let mut perimeter = 0.0;
    let mut correction = 0.0;
    operand.for_each_ring(&mut |ring| {
        for ([start_x, end_x], [start_y, end_y]) in ring
            .xs()
            .array_windows::<2>()
            .zip(ring.ys().array_windows::<2>())
        {
            let start = Point::new_raw_xy(*start_x, *start_y);
            let end = Point::new_raw_xy(*end_x, *end_y);
            let edge = metric.chart_edge(start, end)?;
            add_compensated(
                &mut perimeter,
                &mut correction,
                metric.chart_edge_length(&edge)?,
            );
        }
        Ok(())
    })?;
    Ok(canonicalize_zero(perimeter))
}

fn perimeter_screen_passes(
    operand: &PerimeterOperand<'_>,
    frame: GeographicFrame,
    cached_bounds: Option<Bounds>,
) -> bool {
    let bounds = cached_bounds
        .map(|bounds| (bounds.minx(), bounds.maxx()))
        .or_else(|| operand.longitude_bounds());
    let Some((minimum, maximum)) = bounds else {
        return true;
    };
    let half_period = frame.longitude().half_period();
    minimum.abs() < half_period && maximum.abs() < half_period && maximum - minimum < half_period
}

#[derive(Default)]
struct LongitudeBounds {
    minimum: Option<f64>,
    maximum: Option<f64>,
}

impl LongitudeBounds {
    fn fold(&mut self, longitudes: &[f64]) {
        let Some((&first, rest)) = longitudes.split_first() else {
            return;
        };
        let mut minimum = first;
        let mut maximum = first;
        for &longitude in rest {
            minimum = minimum.min(longitude);
            maximum = maximum.max(longitude);
        }
        self.minimum = Some(self.minimum.map_or(minimum, |old| old.min(minimum)));
        self.maximum = Some(self.maximum.map_or(maximum, |old| old.max(maximum)));
    }
}

struct LiftedVertex {
    point: Point,
    longitude: ExactRational,
    latitude: ExactRational,
    turn: i64,
    seam: bool,
}

struct RingData<'a> {
    source: PerimeterRing<'a>,
    orientation: Option<i8>,
    lifted_extent_at_least_period: bool,
}

struct SeamPiece {
    ring: usize,
    lower: ExactRational,
    upper: ExactRational,
    direction: i8,
}

fn scan_ring(
    metric: &impl ChartMetric,
    frame: GeographicFrame,
    ring_index: usize,
    ring: &PerimeterRing<'_>,
    seam_pieces: &mut Vec<SeamPiece>,
    perimeter: &mut f64,
    correction: &mut f64,
) -> Result<()> {
    let axis = frame.longitude();
    let ordinary_bound = axis.ordinary_longitude_bound();
    let negative_seam_bits = (-axis.half_period()).to_bits();
    let is_seam = |longitude: f64| {
        if longitude.to_bits() == negative_seam_bits {
            true
        } else if longitude.abs() < ordinary_bound {
            false
        } else {
            axis.canonicalize(longitude)
                .expect("geographic perimeter receives an admitted longitude")
                .identity(axis)
                == SeamIdentity::Seam
        }
    };
    let mut start_seam = is_seam(ring.first_point().x);
    for (&[start_x, end_x], &[start_y, end_y]) in ring
        .xs()
        .array_windows::<2>()
        .zip(ring.ys().array_windows::<2>())
    {
        let start = Point::new_raw_xy(start_x, start_y);
        let end = Point::new_raw_xy(end_x, end_y);
        let start_latitude = Latitude::from_admitted(frame, start.y).get();
        let end_latitude = Latitude::from_admitted(frame, end.y).get();
        let end_seam = is_seam(end.x);
        let edge = metric.chart_edge(start, end)?;
        add_compensated(perimeter, correction, metric.chart_edge_length(&edge)?);
        let seam_edge = start_seam && end_seam;
        if seam_edge
            && start_latitude.to_bits() != end_latitude.to_bits()
            && !is_pole_line(start_latitude, end_latitude, frame.pole())
        {
            let first_latitude = ExactRational::from_f64(start_latitude);
            let second_latitude = ExactRational::from_f64(end_latitude);
            let (lower, upper, direction) = if start_latitude < end_latitude {
                (first_latitude, second_latitude, 1)
            } else {
                (second_latitude, first_latitude, -1)
            };
            seam_pieces.push(SeamPiece {
                ring: ring_index,
                lower,
                upper,
                direction,
            });
        }
        start_seam = end_seam;
    }
    Ok(())
}

const fn is_pole_line(start_latitude: f64, end_latitude: f64, pole: f64) -> bool {
    start_latitude.to_bits() == end_latitude.to_bits()
        && start_latitude.abs().to_bits() == pole.to_bits()
}

fn lifted_longitude(longitude: f64, turn: i64, period: f64) -> Result<ExactDyadic> {
    let shifted_period = ExactDyadic::from_f64(period).try_product(&ExactDyadic::from_i64(turn))?;
    ExactDyadic::from_f64(longitude).try_add(shifted_period)
}

fn for_each_lifted_edge(
    frame: GeographicFrame,
    ring: &PerimeterRing<'_>,
    mut visit: impl FnMut(&LiftedVertex, &LiftedVertex) -> Result<()>,
) -> Result<()> {
    let axis = frame.longitude();
    let first = ring.first_point();
    let mut previous_canonical = axis
        .canonicalize(first.x)
        .expect("geographic perimeter receives an admitted longitude");
    let mut chart_turn = 0_i64;
    let mut previous = LiftedVertex {
        point: first,
        longitude: ExactRational::from_dyadic(lifted_longitude(
            previous_canonical.get(),
            chart_turn,
            axis.period(),
        )?),
        latitude: ExactRational::from_f64(Latitude::from_admitted(frame, first.y).get()),
        turn: chart_turn,
        seam: previous_canonical.identity(axis) == SeamIdentity::Seam,
    };
    for (&[start_x, end_x], &[start_y, end_y]) in ring
        .xs()
        .array_windows::<2>()
        .zip(ring.ys().array_windows::<2>())
    {
        let start = Point::new_raw_xy(start_x, start_y);
        let end = Point::new_raw_xy(end_x, end_y);
        let step = axis
            .chart_step(start.x, end.x)
            .expect("geographic perimeter receives admitted longitudes");
        let canonical_end = axis
            .canonicalize(end.x)
            .expect("geographic perimeter receives an admitted longitude");
        chart_turn = chart_turn
            .checked_add(i64::from(
                step.transit_from_canonical(previous_canonical, canonical_end),
            ))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let next = LiftedVertex {
            point: end,
            longitude: ExactRational::from_dyadic(lifted_longitude(
                canonical_end.get(),
                chart_turn,
                axis.period(),
            )?),
            latitude: ExactRational::from_f64(Latitude::from_admitted(frame, end.y).get()),
            turn: chart_turn,
            seam: canonical_end.identity(axis) == SeamIdentity::Seam,
        };
        visit(&previous, &next)?;
        previous = next;
        previous_canonical = canonical_end;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum LineKey {
    Vertical(ExactRational),
    Sloped {
        slope: ExactRational,
        intercept: ExactRational,
    },
}

fn line_key(
    start_longitude: &ExactRational,
    start_latitude: &ExactRational,
    end_longitude: &ExactRational,
    end_latitude: &ExactRational,
) -> Result<LineKey> {
    let longitude_delta = end_longitude.try_subtract(start_longitude)?;
    if longitude_delta.is_zero() {
        return Ok(LineKey::Vertical(start_longitude.clone()));
    }
    let latitude_delta = end_latitude.try_subtract(start_latitude)?;
    let slope = latitude_delta.try_quotient(&longitude_delta)?;
    let intercept = start_latitude.try_subtract(&slope.try_product(start_longitude)?)?;
    Ok(LineKey::Sloped { slope, intercept })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LineRingKey {
    line: LineKey,
    ring: usize,
}

impl Hash for LineRingKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ring.hash(state);
        self.line.hash(state);
    }
}

struct ArcSource {
    edge: ChartEdge,
    start_longitude: ExactRational,
    end_longitude: ExactRational,
    circle: bool,
    parallel: bool,
}

struct ArcPiece {
    lower: ExactRational,
    upper: ExactRational,
    direction: i8,
    lift: i64,
    source: usize,
}

struct ArcGroup {
    line: LineKey,
    ring: usize,
    pieces: Vec<ArcPiece>,
}

fn in_ring_arc_groups(
    metric: &impl ChartMetric,
    frame: GeographicFrame,
    rings: &[RingData<'_>],
) -> Result<(Vec<ArcGroup>, Vec<ArcSource>)> {
    let mut sources = Vec::new();
    let mut groups = Vec::new();
    let mut group_indices = crate::collections::try_hash_map()?;
    for (ring_index, ring) in rings.iter().enumerate() {
        if !ring.lifted_extent_at_least_period {
            continue;
        }
        let axis = frame.longitude();
        for_each_lifted_edge(frame, &ring.source, |start, end| {
            if is_pole_line(start.latitude.round()?, end.latitude.round()?, frame.pole())
                || (start.seam && end.seam)
            {
                return Ok(());
            }

            let edge = metric.chart_edge(start.point, end.point)?;
            let step = axis
                .chart_step(start.point.x, end.point.x)
                .expect("geographic perimeter receives admitted longitudes");
            let source_index = sources.len();
            sources.push(ArcSource {
                edge,
                start_longitude: start.longitude.clone(),
                end_longitude: end.longitude.clone(),
                circle: step.is_circle(),
                parallel: start.latitude == end.latitude,
            });
            append_window_pieces(
                frame,
                ring_index,
                start,
                end,
                source_index,
                &mut groups,
                &mut group_indices,
            )?;
            Ok(())
        })?;
    }
    Ok((groups, sources))
}

fn append_window_pieces(
    frame: GeographicFrame,
    ring: usize,
    start: &LiftedVertex,
    end: &LiftedVertex,
    source: usize,
    groups: &mut Vec<ArcGroup>,
    group_indices: &mut crate::collections::HashMap<LineRingKey, usize>,
) -> Result<()> {
    let axis = frame.longitude();
    let delta_order = end.longitude.try_cmp(&start.longitude)?;
    let mut window = start.turn;
    if start.seam && delta_order == Ordering::Less {
        window = window
            .checked_sub(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }

    if delta_order == Ordering::Equal {
        push_arc_piece(
            frame,
            ring,
            &start.longitude,
            &start.latitude,
            &end.longitude,
            &end.latitude,
            window,
            source,
            groups,
            group_indices,
        )?;
        return Ok(());
    }

    let direction = if delta_order == Ordering::Less {
        -1_i8
    } else {
        1_i8
    };
    let mut piece_start_longitude = start.longitude.clone();
    let mut piece_start_latitude = start.latitude.clone();
    loop {
        let boundary = seam_boundary(axis.half_period(), axis.period(), window, direction)?;
        let crosses = if direction > 0 {
            boundary.try_cmp(&end.longitude)?.is_lt()
        } else {
            boundary.try_cmp(&end.longitude)?.is_gt()
        };
        if !crosses {
            push_arc_piece(
                frame,
                ring,
                &piece_start_longitude,
                &piece_start_latitude,
                &end.longitude,
                &end.latitude,
                window,
                source,
                groups,
                group_indices,
            )?;
            break;
        }

        let denominator = end.longitude.try_subtract(&start.longitude)?;
        let numerator = boundary.try_subtract(&start.longitude)?;
        let parameter = numerator.try_quotient(&denominator)?;
        let latitude_delta = end.latitude.try_subtract(&start.latitude)?;
        let boundary_latitude = start
            .latitude
            .try_add(&latitude_delta.try_product(&parameter)?)?;
        push_arc_piece(
            frame,
            ring,
            &piece_start_longitude,
            &piece_start_latitude,
            &boundary,
            &boundary_latitude,
            window,
            source,
            groups,
            group_indices,
        )?;
        piece_start_longitude = boundary;
        piece_start_latitude = boundary_latitude;
        window = window
            .checked_add(i64::from(direction))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    Ok(())
}

fn seam_boundary(
    half_period: f64,
    period: f64,
    window: i64,
    direction: i8,
) -> Result<ExactRational> {
    let side = if direction > 0 {
        half_period
    } else {
        -half_period
    };
    let shifted = ExactDyadic::from_f64(period).try_product(&ExactDyadic::from_i64(window))?;
    Ok(ExactRational::from_dyadic(
        ExactDyadic::from_f64(side).try_add(shifted)?,
    ))
}

fn push_arc_piece(
    frame: GeographicFrame,
    ring: usize,
    start_longitude: &ExactRational,
    start_latitude: &ExactRational,
    end_longitude: &ExactRational,
    end_latitude: &ExactRational,
    window: i64,
    source: usize,
    groups: &mut Vec<ArcGroup>,
    group_indices: &mut crate::collections::HashMap<LineRingKey, usize>,
) -> Result<()> {
    if start_longitude == end_longitude && start_latitude == end_latitude {
        return Ok(());
    }
    let period_offset = ExactDyadic::from_f64(frame.longitude().period())
        .try_product(&ExactDyadic::from_i64(window))?;
    let offset = ExactRational::from_dyadic(period_offset);
    let mapped_start = start_longitude.try_subtract(&offset)?;
    let mapped_end = end_longitude.try_subtract(&offset)?;
    let line = line_key(&mapped_start, start_latitude, &mapped_end, end_latitude)?;
    let (parameter_start, parameter_end) = match &line {
        LineKey::Vertical(_) => (start_latitude.clone(), end_latitude.clone()),
        LineKey::Sloped { .. } => (mapped_start, mapped_end),
    };
    let direction = if parameter_start.try_cmp(&parameter_end)?.is_lt() {
        1
    } else {
        -1
    };
    let (lower, upper) = if direction > 0 {
        (parameter_start, parameter_end)
    } else {
        (parameter_end, parameter_start)
    };
    let key = LineRingKey {
        line: line.clone(),
        ring,
    };
    let group_index = if let Some(&index) = group_indices.get(&key) {
        index
    } else {
        let index = groups.len();
        groups.push(ArcGroup {
            line,
            ring,
            pieces: Vec::new(),
        });
        group_indices.insert(key, index);
        index
    };
    groups
        .get_mut(group_index)
        .expect("line-ring group index was inserted")
        .pieces
        .push(ArcPiece {
            lower,
            upper,
            direction,
            lift: window,
            source,
        });
    Ok(())
}

fn compare_line_keys(left: &LineKey, right: &LineKey) -> Result<Ordering> {
    match (left, right) {
        (LineKey::Vertical(left), LineKey::Vertical(right)) => left.try_cmp(right),
        (LineKey::Vertical(_), LineKey::Sloped { .. }) => Ok(Ordering::Less),
        (LineKey::Sloped { .. }, LineKey::Vertical(_)) => Ok(Ordering::Greater),
        (
            LineKey::Sloped {
                slope: left_slope,
                intercept: left_intercept,
            },
            LineKey::Sloped {
                slope: right_slope,
                intercept: right_intercept,
            },
        ) => {
            let slope_order = left_slope.try_cmp(right_slope)?;
            if slope_order == Ordering::Equal {
                left_intercept.try_cmp(right_intercept)
            } else {
                Ok(slope_order)
            }
        },
    }
}

fn sort_arc_groups(groups: &mut [ArcGroup]) -> Result<()> {
    let mut failure = None;
    groups.sort_unstable_by(|left, right| {
        if failure.is_some() {
            return Ordering::Equal;
        }
        match compare_line_keys(&left.line, &right.line) {
            Ok(Ordering::Equal) => left.ring.cmp(&right.ring),
            Ok(order) => order,
            Err(error) => {
                failure = Some(error);
                Ordering::Equal
            },
        }
    });
    failure.map_or(Ok(()), Err)
}

struct SweepEvent {
    position: ExactRational,
    piece: usize,
    starts: bool,
}

fn sort_sweep_events(events: &mut [SweepEvent]) -> Result<()> {
    let mut failure = None;
    events.sort_unstable_by(|left, right| {
        if failure.is_some() {
            return Ordering::Equal;
        }
        match left.position.try_cmp(&right.position) {
            Ok(order) => order,
            Err(error) => {
                failure = Some(error);
                Ordering::Equal
            },
        }
    });
    failure.map_or(Ok(()), Err)
}

fn sweep_pieces(
    pieces: &[ArcPiece],
    mut visit_atom: impl FnMut(&ExactRational, &ExactRational, &BTreeSet<usize>) -> Result<()>,
) -> Result<()> {
    if pieces.len() < 2 {
        return Ok(());
    }
    let mut events = Vec::with_capacity(pieces.len().saturating_mul(2));
    for (index, piece) in pieces.iter().enumerate() {
        events.push(SweepEvent {
            position: piece.lower.clone(),
            piece: index,
            starts: true,
        });
        events.push(SweepEvent {
            position: piece.upper.clone(),
            piece: index,
            starts: false,
        });
    }
    sort_sweep_events(&mut events)?;
    let mut active = BTreeSet::new();
    let mut previous: Option<ExactRational> = None;
    let mut cursor = 0;
    while cursor < events.len() {
        let position = events
            .get(cursor)
            .expect("cursor lies in the sorted sweep events")
            .position
            .clone();
        if let Some(start) = previous.as_ref()
            && start.try_cmp(&position)?.is_lt()
            && !active.is_empty()
        {
            visit_atom(start, &position, &active)?;
        }
        while events
            .get(cursor)
            .is_some_and(|event| event.position == position)
        {
            let event = events
                .get(cursor)
                .expect("cursor lies in the sorted sweep events");
            if event.starts {
                active.insert(event.piece);
            } else {
                active.remove(&event.piece);
            }
            cursor = cursor
                .checked_add(1)
                .expect("sweep event count fits its collection");
        }
        previous = Some(position);
    }
    debug_assert!(active.is_empty());
    Ok(())
}

fn cancel_in_ring_group(
    metric: &impl ChartMetric,
    group: &ArcGroup,
    sources: &[ArcSource],
    perimeter: &mut f64,
    correction: &mut f64,
) -> Result<()> {
    if group.pieces.len() < 2 {
        return Ok(());
    }
    let mut lifts = BTreeMap::<i64, LiftDirections>::new();
    let mut removed_length = 0.0;
    let mut removed_correction = 0.0;
    sweep_pieces(&group.pieces, |lower, upper, active| {
        lifts.clear();
        let mut positive = 0_usize;
        let mut negative = 0_usize;
        for &piece_index in active {
            let piece = group
                .pieces
                .get(piece_index)
                .expect("active piece belongs to its line group");
            let counts = lifts.entry(piece.lift).or_default();
            if piece.direction > 0 {
                counts.positive = counts
                    .positive
                    .checked_add(1)
                    .expect("positive copy count fits its active-piece set");
            } else {
                counts.negative = counts
                    .negative
                    .checked_add(1)
                    .expect("negative copy count fits its active-piece set");
            }
        }
        let mut largest_lift = 0_usize;
        for counts in lifts.values() {
            positive = positive
                .checked_add(counts.positive)
                .expect("positive copy count fits its active-piece set");
            negative = negative
                .checked_add(counts.negative)
                .expect("negative copy count fits its active-piece set");
            let at_lift = counts
                .positive
                .checked_add(counts.negative)
                .expect("lift copy count fits its active-piece set");
            largest_lift = largest_lift.max(at_lift);
        }
        let pairs = positive
            .min(negative)
            .min(active.len().saturating_sub(largest_lift));
        if pairs == 0 {
            return Ok(());
        }
        let removed = pairs
            .checked_mul(2)
            .expect("cancellation removes two copies per matched pair");
        let atom = arc_atom_length(metric, group, sources, active, lower, upper)?;
        add_compensated(
            &mut removed_length,
            &mut removed_correction,
            -atom * removed as f64,
        );
        Ok(())
    })?;
    add_compensated(perimeter, correction, removed_length);
    Ok(())
}

fn arc_atom_length(
    metric: &impl ChartMetric,
    group: &ArcGroup,
    sources: &[ArcSource],
    active: &BTreeSet<usize>,
    lower: &ExactRational,
    upper: &ExactRational,
) -> Result<f64> {
    let piece_index = active
        .first()
        .copied()
        .expect("a perimeter atom has at least one active piece");
    let piece = group
        .pieces
        .get(piece_index)
        .expect("active piece belongs to its line group");
    let source = sources
        .get(piece.source)
        .expect("arc piece belongs to its source table");
    if source.circle && source.parallel {
        let full_circle = metric.chart_edge_length(&source.edge)?;
        let width = upper.try_subtract(lower)?;
        let fraction = width.try_quotient(&ExactRational::from_f64(
            metric.chart_frame().longitude().period(),
        ))?;
        return Ok(full_circle * fraction.round()?);
    }

    if let LineKey::Vertical(longitude) = &group.line {
        let edge = metric.chart_edge(
            Point::new_raw_xy(longitude.round()?, lower.round()?),
            Point::new_raw_xy(longitude.round()?, upper.round()?),
        )?;
        return metric.chart_edge_length(&edge);
    }
    let lower_parameter = source_parameter(
        source,
        piece.lift,
        lower,
        metric.chart_frame().longitude().period(),
    )?;
    let upper_parameter = source_parameter(
        source,
        piece.lift,
        upper,
        metric.chart_frame().longitude().period(),
    )?;
    let (start, end) = if lower_parameter < upper_parameter {
        (lower_parameter, upper_parameter)
    } else {
        (upper_parameter, lower_parameter)
    };
    metric.chart_edge_interval_length(&source.edge, start, end)
}

fn source_parameter(
    source: &ArcSource,
    window: i64,
    position: &ExactRational,
    period: f64,
) -> Result<f64> {
    let window_offset = ExactRational::from_dyadic(
        ExactDyadic::from_f64(period).try_product(&ExactDyadic::from_i64(window))?,
    );
    let lifted_position = position.try_add(&window_offset)?;
    let numerator = lifted_position.try_subtract(&source.start_longitude)?;
    let denominator = source.end_longitude.try_subtract(&source.start_longitude)?;
    numerator.try_quotient(&denominator)?.round()
}

fn cancel_seam_pieces(
    metric: &impl ChartMetric,
    frame: GeographicFrame,
    rings: &mut [RingData<'_>],
    pieces: &[SeamPiece],
    perimeter: &mut f64,
    correction: &mut f64,
) -> Result<()> {
    if pieces.len() < 2 {
        return Ok(());
    }
    let arcs = pieces
        .iter()
        .map(|piece| ArcPiece {
            lower: piece.lower.clone(),
            upper: piece.upper.clone(),
            direction: piece.direction,
            lift: 0,
            source: piece.ring,
        })
        .collect::<Vec<_>>();
    let mut fluxes = vec![RingFlux::default(); rings.len()];
    let mut touched = Vec::new();
    sweep_pieces(&arcs, |south, north, active| {
        touched.clear();
        for &piece_index in active {
            let piece = pieces
                .get(piece_index)
                .expect("active piece belongs to the seam sweep");
            let flux = fluxes
                .get_mut(piece.ring)
                .expect("seam piece belongs to its operand ring");
            if flux.cover == 0 {
                touched.push(piece.ring);
            }
            flux.cover = flux
                .cover
                .checked_add(1)
                .expect("seam copy count fits its active-piece set");
            flux.net = flux
                .net
                .checked_add(i64::from(piece.direction))
                .expect("seam winding fits its active-piece set");
        }

        let cover = active.len();
        let retained = if touched.len() == 1 {
            let ring_index = *touched
                .first()
                .expect("active seam interval has at least one ring");
            u128::from(
                fluxes
                    .get(ring_index)
                    .expect("seam ring index fits its flux table")
                    .net
                    .unsigned_abs(),
            )
        } else {
            let mut net = 0_i128;
            let mut undetermined = 0_u128;
            for &ring_index in &touched {
                let ring = rings
                    .get_mut(ring_index)
                    .expect("seam ring index fits its orientation table");
                let epsilon = i128::from(ring.source.role)
                    .checked_mul(i128::from(
                        ring.orientation
                            .expect("multi-ring seam atom has a ring orientation"),
                    ))
                    .expect("shell/hole orientation product fits i128");
                let winding = i128::from(
                    fluxes
                        .get(ring_index)
                        .expect("seam ring index fits its flux table")
                        .net,
                );
                if epsilon == 0 {
                    undetermined = undetermined
                        .checked_add(winding.unsigned_abs())
                        .expect("seam copy count fits its active-piece set");
                } else {
                    net = net
                        .checked_add(
                            epsilon
                                .checked_mul(winding)
                                .expect("orientation-normalized seam flux fits i128"),
                        )
                        .expect("orientation-normalized seam flux fits i128");
                }
            }
            net.unsigned_abs()
                .checked_add(undetermined)
                .expect("retained seam copy count fits its coverage")
        };
        let coverage = u128::try_from(cover).expect("usize count fits u128");
        let removed = coverage
            .checked_sub(retained)
            .expect("retained seam copies do not exceed total coverage");
        for &ring_index in &touched {
            *fluxes
                .get_mut(ring_index)
                .expect("seam ring index fits its flux table") = RingFlux::default();
        }
        touched.clear();
        if removed == 0 {
            return Ok(());
        }
        let south = south.round()?;
        let north = north.round()?;
        let edge = metric.chart_edge(
            Point::new_raw_xy(-frame.longitude().half_period(), south),
            Point::new_raw_xy(-frame.longitude().half_period(), north),
        )?;
        let length = metric.chart_edge_length(&edge)?;
        add_compensated(perimeter, correction, -(length * removed as f64));
        Ok(())
    })?;
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct RingFlux {
    cover: usize,
    net: i64,
}

#[derive(Clone, Copy, Default)]
struct LiftDirections {
    positive: usize,
    negative: usize,
}

fn add_compensated(sum: &mut f64, correction: &mut f64, value: f64) {
    let adjusted = value - *correction;
    let next = *sum + adjusted;
    *correction = (next - *sum) - adjusted;
    *sum = next;
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{PerimeterOperand, line_key, perimeter_screen_passes};
    use crate::geographic::GeographicFrame;
    use crate::geometry::tessellation::exact::ExactRational;
    use crate::geometry::{Bounds, Point, Polygon, Ring, Shape};

    fn longitude_box(west: f64, east: f64) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::closed(vec![
                Point::new_raw_xy(west, 0.0),
                Point::new_raw_xy(east, 0.0),
                Point::new_raw_xy(east, 1.0),
                Point::new_raw_xy(west, 1.0),
            ])
            .unwrap(),
            Vec::new(),
        ))
    }

    fn screen(west: f64, east: f64, cached: Option<Bounds>) -> bool {
        let shape = longitude_box(west, east);
        perimeter_screen_passes(
            &PerimeterOperand::Shape(&shape),
            GeographicFrame::crs84(),
            cached,
        )
    }

    #[test]
    fn perimeter_screen_uses_strict_coordinate_and_extent_bounds() {
        assert!(screen(-89.0, 89.0, None));
        assert!(!screen(-90.0, 90.0, None));
        assert!(!screen(170.0, 180.0, None));
        assert!(!screen(-180.0, -170.0, None));
        assert!(!screen(530.0, 540.0, None));
        let cached = Bounds::new_raw(-89.0, 0.0, 89.0, 1.0);
        assert!(screen(-89.0, 89.0, Some(cached)));
        let cached_seam = Bounds::new_raw(530.0, 0.0, 540.0, 1.0);
        assert!(!screen(530.0, 540.0, Some(cached_seam)));
    }

    #[test]
    fn line_keys_group_collinear_edges_with_different_ratio_exponents() {
        let value = ExactRational::from_f64;
        let noded = line_key(&value(114.0), &value(-63.0), &value(116.0), &value(-69.5)).unwrap();
        let unnoded = line_key(&value(114.0), &value(-63.0), &value(118.0), &value(-76.0)).unwrap();

        assert_eq!(noded, unnoded);
        let mut keys = HashSet::new();
        assert!(keys.insert(noded));
        assert!(!keys.insert(unnoded));
    }
}
