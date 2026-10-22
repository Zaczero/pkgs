//! Conversion between periodic source charts and literal planar wire drawings.

use crate::error::{ResourceError, Result};
use crate::geographic::retained_exact::sites::round_native_longitude_with_work;
use crate::geographic::{ChartStep, EdgeDirection, GeographicFrame, Latitude};
use crate::geometry::{
    ChartMetric, CoordSeq, CoordSeqBuilder, Coordinates as _, ExactDyadic, ExactRational,
    ExpansionBudget, GeometryErrorKind, LineSeq, MOrdinate, Point, Shape, SourceArcMetricAccess,
    ZOrdinate, interpolate_subdivision_ordinate,
};
use crate::work::Unmetered;

pub(crate) fn geographic_interchange_ready(shape: &Shape, frame: GeographicFrame) -> bool {
    let half = frame.longitude().half_period();
    let pole = frame.pole();
    let mut ready = true;
    shape.for_each_first_xy_point_run(|point| {
        ready &= point.x >= -half && point.x <= half && point.y >= -pole && point.y <= pole;
    });
    shape.for_each_vertex_pair(|start, end| {
        let span = (end.x - start.x).abs();
        ready &= span < half
            || (span.to_bits() == half.to_bits()
                && matches!(
                    frame.longitude().chart_step(start.x, end.x),
                    Ok(ChartStep::Shortest(EdgeDirection::HalfTurn(_)))
                ));
    });
    ready
}

pub(crate) fn geographic_interchange_drawing<M: ChartMetric>(
    shape: &Shape,
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    if geographic_interchange_ready(shape, frame) {
        return Ok(shape.clone());
    }
    let drawing = shape.interchange_drawing_in_frame(frame, metric_access)?;
    let (drawing, _) = drawing.finish_scalar(crate::Frame::None)?.into_scalar()?;
    drawing.map_xy_with(|x, y| canonical_xy(x, y, frame))
}

#[expect(
    clippy::float_cmp,
    reason = "the positive seam spelling closes the east wire chart"
)]
fn canonical_xy(x: f64, y: f64, frame: GeographicFrame) -> Result<(f64, f64)> {
    let axis = frame.longitude();
    let x = if x == axis.half_period() {
        x
    } else {
        axis.canonicalize(x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
            .get()
    };
    let y = Latitude::try_new(frame, y)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
        .get();
    Ok((x, y))
}

fn canonical_point(point: Point, frame: GeographicFrame) -> Result<Point> {
    let (x, y) = canonical_xy(point.x, point.y, frame)?;
    Ok(point.with_xy_raw(x, y))
}

fn literal_piece_count(
    start: Point,
    end: Point,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<usize> {
    let half_span = (end.x * 0.5 - start.x * 0.5).abs();
    let half_period = frame.longitude().half_period();
    if half_span < half_period * 0.5 {
        return Ok(1);
    }
    // Dyadic subdivisions stay well inside a half turn after rounding, so a
    // canonicalized endpoint pair cannot select the opposite half-turn branch.
    let mut pieces = 2_usize;
    while half_span / pieces as f64 > half_period * 0.25 {
        pieces = pieces.checked_mul(2).ok_or(ResourceError::SizeOverflow)?;
        budget.check_additional(pieces - 1)?;
    }
    budget.add(pieces - 1)?;
    crate::geometry::types::ensure_coordseq_vertex_capacity(pieces)?;
    Ok(pieces)
}

fn literal_vertex(
    start: Point,
    end: Point,
    index: usize,
    pieces: usize,
    frame: GeographicFrame,
) -> Result<Point> {
    let exponent = -i64::from(pieces.trailing_zeros());
    let parameter = ExactDyadic::try_from_u128(false, index as u128, exponent)?;
    let longitude = ExactDyadic::from_f64(end.x)
        .try_subtract(ExactDyadic::from_f64(start.x))?
        .try_product(&parameter)?
        .try_add(ExactDyadic::from_f64(start.x))?;
    // Wrap the mathematical site before binary64 rounding. Interpolating a
    // large longitude alias as f64 first can erase all intermediate turns.
    let x = round_native_longitude_with_work(
        frame.longitude(),
        &ExactRational::from_dyadic(longitude),
        &mut Unmetered,
    )?;
    let fraction = index as f64 / pieces as f64;
    let y = interpolate_subdivision_ordinate(start.y, end.y, fraction);
    let z = start
        .z()
        .zip(end.z())
        .map(|(start, end)| interpolate_subdivision_ordinate(start, end, fraction));
    let m = start
        .m()
        .zip(end.m())
        .map(|(start, end)| interpolate_subdivision_ordinate(start, end, fraction));
    Point::new_axes(x, y, ZOrdinate(z), MOrdinate(m))
}

fn admit_literal_coords(
    coords: &CoordSeq,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    let mut output = CoordSeqBuilder::with_capacity(coords.axes(), coords.len());
    let mut points = coords.iter_coords();
    let Some(mut start) = points.next() else {
        return Ok(coords.clone());
    };
    output.push(canonical_point(start, frame)?);
    for end in points {
        let pieces = literal_piece_count(start, end, frame, budget)?;
        for index in 1..pieces {
            output.push(literal_vertex(start, end, index, pieces, frame)?);
        }
        output.push(canonical_point(end, frame)?);
        start = end;
    }
    output.finish()
}

pub(crate) fn admit_literal_geographic_shape(
    shape: &Shape,
    frame: GeographicFrame,
) -> Result<Shape> {
    admit_literal_shape_with_budget(
        shape,
        frame,
        &mut ExpansionBudget::new("geographic interchange", "literal edge subdivision"),
    )
}

fn admit_literal_shape_with_budget(
    shape: &Shape,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    if geographic_interchange_ready(shape, frame) {
        return Ok(shape.clone());
    }
    Ok(match shape {
        Shape::Point(point) => Shape::Point(canonical_point(*point, frame)?),
        Shape::MultiPoint(points) => {
            Shape::MultiPoint(points.try_map_xy_with(|x, y| canonical_xy(x, y, frame))?)
        },
        Shape::LineString(line) => Shape::LineString(LineSeq::try_new(admit_literal_coords(
            line.as_coords(),
            frame,
            budget,
        )?)?),
        Shape::MultiLineString(lines) => {
            Shape::MultiLineString(lines.try_map_values(lines.axes(), |line| {
                LineSeq::try_new(admit_literal_coords(line.as_coords(), frame, budget)?)
            })?)
        },
        Shape::Polygon(polygon) => Shape::Polygon(
            polygon.try_map_ring_seqs(|coords| admit_literal_coords(coords, frame, budget))?,
        ),
        Shape::MultiPolygon(polygons) => {
            Shape::MultiPolygon(polygons.try_map_values(polygons.axes(), |polygon| {
                polygon.try_map_ring_seqs(|coords| admit_literal_coords(coords, frame, budget))
            })?)
        },
        Shape::GeometryCollection(collection) => Shape::GeometryCollection(
            collection
                .try_map_members(|shape| admit_literal_shape_with_budget(shape, frame, budget))?,
        ),
        Shape::Empty(..) => shape.clone(),
    })
}
