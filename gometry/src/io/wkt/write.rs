use crate::geometry::GeometryErrorKind;
use crate::io::wkt::{
    CoordinateAxes, Coordinates, GeometryKind, Point, Polygon, Result, Shape, extended_srid_code,
    require_serializable_axes,
};
use crate::io::{CheckedText, wkt_text_summary};

trait WktSink {
    fn len(&self) -> usize;
    fn push_str(&mut self, value: &str) -> Result<()>;
    fn push(&mut self, value: char) -> Result<()>;
    fn write_formatted(&mut self, arguments: std::fmt::Arguments<'_>) -> Result<()>;
    fn write_number(&mut self, value: f64, format: WktNumberFormat) -> Result<()>;
}

impl WktSink for String {
    fn len(&self) -> usize {
        self.len()
    }

    fn push_str(&mut self, value: &str) -> Result<()> {
        self.push_str(value);
        Ok(())
    }

    fn push(&mut self, value: char) -> Result<()> {
        self.push(value);
        Ok(())
    }

    fn write_formatted(&mut self, arguments: std::fmt::Arguments<'_>) -> Result<()> {
        std::fmt::write(self, arguments).expect("writing WKT to String");
        Ok(())
    }

    fn write_number(&mut self, value: f64, format: WktNumberFormat) -> Result<()> {
        format.write(self, value);
        Ok(())
    }
}

struct CheckedWkt {
    text: CheckedText,
    number: String,
}

impl CheckedWkt {
    fn with_capacity_hint(capacity: usize, limit: usize) -> Self {
        Self {
            text: CheckedText::with_capacity_hint(capacity, limit),
            number: String::new(),
        }
    }
}

impl WktSink for CheckedWkt {
    fn len(&self) -> usize {
        self.text.len()
    }

    fn push_str(&mut self, value: &str) -> Result<()> {
        self.text.push_str(value)
    }

    fn push(&mut self, value: char) -> Result<()> {
        self.text.push(value)
    }

    fn write_formatted(&mut self, arguments: std::fmt::Arguments<'_>) -> Result<()> {
        self.text.write_formatted(arguments)
    }

    fn write_number(&mut self, value: f64, format: WktNumberFormat) -> Result<()> {
        format.write_checked(&mut self.text, &mut self.number, value)
    }
}

pub(crate) fn to_wkt(shape: &Shape) -> Result<String> {
    require_serializable_axes(shape)?;
    format_wkt(shape, WktNumberFormat::Shortest)
}

fn wkt_srid_prefix(crs: Option<&str>, include_srid: bool) -> Result<Option<String>> {
    extended_srid_code(crs, include_srid, "EWKT")
        .map(|code| code.map(|code| format!("SRID={code};")))
}

pub(crate) fn to_wkt_with_srid(
    shape: &Shape,
    crs: Option<&str>,
    include_srid: bool,
) -> Result<String> {
    // Reject heterogeneous-axis multiparts before inventing missing Z/M as 0.
    require_serializable_axes(shape)?;
    // Preserve body-error precedence over a malformed SRID prefix.
    let prefix = wkt_srid_prefix(crs, include_srid);
    let body_limit = match &prefix {
        Ok(Some(prefix)) => isize::MAX.unsigned_abs().saturating_sub(prefix.len()),
        Ok(None) | Err(_) => isize::MAX.unsigned_abs(),
    };
    let body = format_wkt_with_limit(shape, WktNumberFormat::Shortest, body_limit)?;
    match prefix? {
        Some(prefix) => {
            let length = prefix
                .len()
                .checked_add(body.len())
                .filter(|length| isize::try_from(*length).is_ok())
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let mut output = String::with_capacity(length);
            output.push_str(&prefix);
            output.push_str(&body);
            Ok(output)
        },
        None => Ok(body),
    }
}

/// How `format(geom, spec)` renders WKT ordinates — display-only control,
/// never a precision model.
#[derive(Clone, Copy)]
pub(crate) enum WktNumberFormat {
    /// Shortest round-trip representation (the default writer).
    Shortest,
    /// Fixed decimal places, trailing zeros kept (``'.3f'``).
    Fixed(u8),
    /// Rounded to decimal places, trailing zeros trimmed (``'.3g'``).
    Trimmed(u8),
}

impl WktNumberFormat {
    pub(super) fn write(self, out: &mut String, value: f64) {
        use std::fmt::Write as _;
        match self {
            Self::Shortest => {
                // Keep plain decimal output outside zmij's compact notation range.
                if value == 0.0 || (1e-5..1e16).contains(&value.abs()) {
                    let mut buffer = zmij::Buffer::new();
                    let digits = buffer.format_finite(value);
                    // std omits ".0" on integral values; zmij keeps it.
                    out.push_str(digits.strip_suffix(".0").unwrap_or(digits));
                } else {
                    write!(out, "{value}").expect("writing a number to String");
                }
            },
            Self::Fixed(precision) | Self::Trimmed(precision) => {
                let start = out.len();
                write!(out, "{value:.*}", usize::from(precision))
                    .expect("writing a number to String");
                if matches!(self, Self::Trimmed(_))
                    && let Some(rendered) = out.get(start..)
                    && rendered.contains('.')
                {
                    let keep = rendered.trim_end_matches('0').trim_end_matches('.').len();
                    out.truncate(start.wrapping_add(keep));
                }
            },
        }
    }

    fn write_checked(self, out: &mut CheckedText, scratch: &mut String, value: f64) -> Result<()> {
        match self {
            Self::Shortest => {
                if value == 0.0 || (1e-5..1e16).contains(&value.abs()) {
                    let mut buffer = zmij::Buffer::new();
                    let digits = buffer.format_finite(value);
                    out.push_str(digits.strip_suffix(".0").unwrap_or(digits))?;
                } else {
                    out.write_formatted(format_args!("{value}"))?;
                }
            },
            Self::Fixed(precision) => {
                out.write_formatted(format_args!("{value:.*}", usize::from(precision)))?;
            },
            Self::Trimmed(precision) => {
                use std::fmt::Write as _;
                // Trim in reusable scratch before charging the checked output.
                scratch.clear();
                write!(scratch, "{value:.*}", usize::from(precision))
                    .expect("writing a formatted number to String");
                let mut end = scratch.len();
                if scratch.as_bytes().contains(&b'.') {
                    while let Some(index) = end
                        .checked_sub(1)
                        .filter(|&index| scratch.as_bytes().get(index) == Some(&b'0'))
                    {
                        end = index;
                    }
                    if let Some(index) = end.checked_sub(1)
                        && scratch.as_bytes().get(index) == Some(&b'.')
                    {
                        end = index;
                    }
                }
                scratch.truncate(end);
                out.push_str(scratch)?;
            },
        }
        Ok(())
    }

    fn maximum_ordinate_bytes(self) -> usize {
        match self {
            // Plain Display stays below 1,100 bytes for any finite binary64.
            Self::Shortest => 1_100,
            Self::Fixed(precision) | Self::Trimmed(precision) => {
                usize::from(precision).saturating_add(311)
            },
        }
    }
}

/// WKT with display-formatted ordinates; backs `Geometry.__format__`.
pub(crate) fn to_wkt_display(shape: &Shape, format: WktNumberFormat) -> Result<String> {
    require_serializable_axes(shape)?;
    format_wkt(shape, format)
}

fn format_wkt(shape: &Shape, format: WktNumberFormat) -> Result<String> {
    format_wkt_with_limit(shape, format, isize::MAX.unsigned_abs())
}

fn format_wkt_with_limit(shape: &Shape, format: WktNumberFormat, limit: usize) -> Result<String> {
    let limit = limit.min(isize::MAX.unsigned_abs());
    let summary = wkt_text_summary(shape);
    summary.wkt_lower_bound(limit).require_within()?;

    // Reserve ordinary decimal widths; extreme values and precision can grow normally.
    let bytes_per_ordinate = match format {
        WktNumberFormat::Shortest => 24,
        WktNumberFormat::Fixed(precision) | WktNumberFormat::Trimmed(precision) => {
            usize::from(precision).checked_add(24).unwrap_or_default()
        },
    };
    let coordinate_bytes_per_coord = shape.axes().dimension().checked_mul(bytes_per_ordinate);
    let capacity = summary
        .coordinate_count()
        .zip(coordinate_bytes_per_coord)
        .and_then(|(count, bytes_per_coord)| count.checked_mul(bytes_per_coord))
        .zip(
            summary
                .structure_count()
                .and_then(|count| count.checked_mul(32)),
        )
        .and_then(|(coordinate_bytes, structural_bytes)| {
            coordinate_bytes.checked_add(structural_bytes)
        })
        .filter(|capacity| isize::try_from(*capacity).is_ok())
        .unwrap_or_default();
    let dimensions = shape.axes().dimension();
    let maximum_ordinate_bytes = format.maximum_ordinate_bytes();
    let base_writer_fits = summary
        .wkt_upper_bound(dimensions, maximum_ordinate_bytes)
        .is_some_and(|upper_bound| upper_bound <= limit);
    if base_writer_fits {
        let mut out = String::new();
        out.reserve(capacity);
        write_wkt(&mut out, shape, format, usize::MAX)?;
        return Ok(out);
    }

    let mut out = CheckedWkt::with_capacity_hint(capacity.min(limit), limit);
    write_wkt(&mut out, shape, format, usize::MAX)?;
    let CheckedWkt { text, .. } = out;
    Ok(text.into_string())
}

/// WKT rendered only far enough to fill `limit` bytes for bounded previews.
pub(crate) fn to_wkt_preview(shape: &Shape, limit: usize) -> Result<String> {
    let capacity = limit
        .checked_add(4096)
        .filter(|capacity| isize::try_from(*capacity).is_ok())
        .unwrap_or_default();
    let mut out = String::new();
    out.reserve(capacity);
    write_wkt(&mut out, shape, WktNumberFormat::Shortest, limit)?;
    Ok(out)
}

fn write_wkt<S: WktSink>(
    out: &mut S,
    shape: &Shape,
    format: WktNumberFormat,
    limit: usize,
) -> Result<()> {
    match shape {
        Shape::Point(point) => {
            let axes = CoordinateAxes::from_point(*point);
            out.write_formatted(format_args!("POINT{} (", axes.wkt_tag()))?;
            write_wkt_point(out, *point, axes, format)?;
            out.push(')')?;
            Ok(())
        },
        Shape::MultiPoint(points) => {
            out.write_formatted(format_args!("MULTIPOINT{} (", shape.axes().wkt_tag()))?;
            for (index, member) in points.members().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ")?;
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY")?,
                    crate::geometry::MultiMember::Value(point_index) => {
                        let point = points.point_at(point_index);
                        out.push('(')?;
                        write_wkt_point(out, point, CoordinateAxes::from_point(point), format)?;
                        out.push(')')?;
                    },
                }
            }
            out.push(')')?;
            Ok(())
        },
        Shape::LineString(points) => {
            let axes = shape.axes();
            out.write_formatted(format_args!("LINESTRING{} (", axes.wkt_tag()))?;
            write_wkt_ring(out, points, axes, format, limit)?;
            out.push(')')?;
            Ok(())
        },
        Shape::MultiLineString(lines) => {
            let axes = shape.axes();
            out.write_formatted(format_args!("MULTILINESTRING{} (", axes.wkt_tag()))?;
            for (index, member) in lines.members_with_values().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ")?;
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY")?,
                    crate::geometry::MultiMember::Value(line) => {
                        require_wkt_axes(line.axes(), axes)?;
                        out.push('(')?;
                        write_wkt_ring(out, line, axes, format, limit)?;
                        out.push(')')?;
                    },
                }
            }
            out.push(')')?;
            Ok(())
        },
        Shape::Polygon(polygon) => {
            let axes = shape.axes();
            out.write_formatted(format_args!("POLYGON{} (", axes.wkt_tag()))?;
            write_wkt_polygon(out, polygon, axes, format, limit)?;
            out.push(')')?;
            Ok(())
        },
        Shape::MultiPolygon(polygons) => {
            let axes = shape.axes();
            out.write_formatted(format_args!("MULTIPOLYGON{} (", axes.wkt_tag()))?;
            for (index, member) in polygons.members_with_values().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ")?;
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY")?,
                    crate::geometry::MultiMember::Value(polygon) => {
                        out.push('(')?;
                        write_wkt_polygon(out, polygon, axes, format, limit)?;
                        out.push(')')?;
                    },
                }
            }
            out.push(')')?;
            Ok(())
        },
        Shape::GeometryCollection(geometries) => {
            out.push_str("GEOMETRYCOLLECTION (")?;
            for (index, geometry) in geometries.iter().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ")?;
                }
                write_wkt(out, geometry, format, limit)?;
            }
            out.push(')')?;
            Ok(())
        },
        Shape::Empty(kind, axes) => {
            let keyword = match kind {
                GeometryKind::Point => "POINT",
                GeometryKind::MultiPoint => "MULTIPOINT",
                GeometryKind::LineString => "LINESTRING",
                GeometryKind::Polygon => "POLYGON",
                GeometryKind::MultiLineString => "MULTILINESTRING",
                GeometryKind::MultiPolygon => "MULTIPOLYGON",
                GeometryKind::GeometryCollection => "GEOMETRYCOLLECTION",
            };
            write_wkt_empty(out, keyword, *axes)?;
            Ok(())
        },
    }
}

fn require_wkt_axes(actual: CoordinateAxes, required: CoordinateAxes) -> Result<()> {
    if (required.has_z() && !actual.has_z()) || (required.has_m() && !actual.has_m()) {
        return Err(GeometryErrorKind::message(format!(
            "WKT output requires {} axes, but this coordinate sequence carries {}",
            required.as_str(),
            actual.as_str(),
        )));
    }
    Ok(())
}

/// `{KEYWORD}{ Z| M| ZM} EMPTY` retains dimensional tags for typed empties.
fn write_wkt_empty<S: WktSink>(out: &mut S, keyword: &str, axes: CoordinateAxes) -> Result<()> {
    out.push_str(keyword)?;
    out.push_str(axes.wkt_tag())?;
    out.push_str(" EMPTY")
}

fn write_wkt_ring<S: WktSink, C: Coordinates + ?Sized>(
    out: &mut S,
    points: &C,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
) -> Result<()> {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        write_wkt_ring_impl::<S, HAS_Z, HAS_M, C>(out, points, format, limit)
    })
}

fn write_wkt_ring_impl<S: WktSink, const Z: bool, const M: bool, C: Coordinates + ?Sized>(
    out: &mut S,
    points: &C,
    format: WktNumberFormat,
    limit: usize,
) -> Result<()> {
    if let Some((xs, ys)) = points.xy_columns() {
        if Z {
            let zs = points.z_column().expect("WKT Z axis was preflighted");
            if M {
                let ms = points.m_column().expect("WKT M axis was preflighted");
                return write_wkt_column_rows::<S, Z, M, _>(
                    out,
                    format,
                    limit,
                    xs.iter()
                        .copied()
                        .zip(ys.iter().copied())
                        .zip(zs.iter().copied())
                        .zip(ms.iter().copied())
                        .map(|(((x, y), z), m)| (x, y, z, m)),
                );
            }
            return write_wkt_column_rows::<S, Z, M, _>(
                out,
                format,
                limit,
                xs.iter()
                    .copied()
                    .zip(ys.iter().copied())
                    .zip(zs.iter().copied())
                    .map(|((x, y), z)| (x, y, z, 0.0)),
            );
        }
        if M {
            let ms = points.m_column().expect("WKT M axis was preflighted");
            return write_wkt_column_rows::<S, Z, M, _>(
                out,
                format,
                limit,
                xs.iter()
                    .copied()
                    .zip(ys.iter().copied())
                    .zip(ms.iter().copied())
                    .map(|((x, y), m)| (x, y, 0.0, m)),
            );
        }
        return write_wkt_column_rows::<S, Z, M, _>(
            out,
            format,
            limit,
            xs.iter()
                .copied()
                .zip(ys.iter().copied())
                .map(|(x, y)| (x, y, 0.0, 0.0)),
        );
    }

    for (index, point) in points.iter_coords().enumerate() {
        if out.len() >= limit {
            break;
        }
        if index > 0 {
            out.push_str(", ")?;
        }
        write_wkt_point_impl::<S, Z, M>(out, point, format)?;
    }
    Ok(())
}

fn write_wkt_column_rows<S: WktSink, const Z: bool, const M: bool, I>(
    out: &mut S,
    format: WktNumberFormat,
    limit: usize,
    rows: I,
) -> Result<()>
where
    I: Iterator<Item = (f64, f64, f64, f64)>,
{
    for (index, (x, y, z, m)) in rows.enumerate() {
        if out.len() >= limit {
            break;
        }
        if index > 0 {
            out.push_str(", ")?;
        }
        write_wkt_coordinate_impl::<S, Z, M>(out, x, y, z, m, format)?;
    }
    Ok(())
}

fn write_wkt_polygon<S: WktSink>(
    out: &mut S,
    polygon: &Polygon,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
) -> Result<()> {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        write_wkt_polygon_impl::<S, HAS_Z, HAS_M>(out, polygon, axes, format, limit)
    })
}

fn write_wkt_polygon_impl<S: WktSink, const Z: bool, const M: bool>(
    out: &mut S,
    polygon: &Polygon,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
) -> Result<()> {
    require_wkt_axes(polygon.shell.axes(), axes)?;
    out.push('(')?;
    write_wkt_ring_impl::<S, Z, M, _>(out, &polygon.shell, format, limit)?;
    out.push(')')?;
    for hole in polygon.holes.iter() {
        if out.len() >= limit {
            break;
        }
        require_wkt_axes(hole.axes(), axes)?;
        out.push_str(", (")?;
        write_wkt_ring_impl::<S, Z, M, _>(out, hole, format, limit)?;
        out.push(')')?;
    }
    Ok(())
}

fn write_wkt_point<S: WktSink>(
    out: &mut S,
    point: Point,
    axes: CoordinateAxes,
    format: WktNumberFormat,
) -> Result<()> {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        write_wkt_point_impl::<S, HAS_Z, HAS_M>(out, point, format)
    })
}

fn write_wkt_point_impl<S: WktSink, const Z: bool, const M: bool>(
    out: &mut S,
    point: Point,
    format: WktNumberFormat,
) -> Result<()> {
    let z = if Z {
        point.z().expect("WKT Z axis was preflighted")
    } else {
        0.0
    };
    let m = if M {
        point.m().expect("WKT M axis was preflighted")
    } else {
        0.0
    };
    write_wkt_coordinate_impl::<S, Z, M>(out, point.x, point.y, z, m, format)
}

fn write_wkt_coordinate_impl<S: WktSink, const Z: bool, const M: bool>(
    out: &mut S,
    x: f64,
    y: f64,
    z: f64,
    m: f64,
    format: WktNumberFormat,
) -> Result<()> {
    out.write_number(x, format)?;
    out.push(' ')?;
    out.write_number(y, format)?;
    if Z {
        out.push(' ')?;
        out.write_number(z, format)?;
    }
    if M {
        out.push(' ')?;
        out.write_number(m, format)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{MOrdinate, ZOrdinate};
    use crate::io::{CoordSeq, Ring, TextWidthBound};

    #[test]
    fn base_writer_upper_bound_covers_each_geometry_kind() {
        for source in [
            "POINT (0 0)",
            "POINT EMPTY",
            "MULTIPOINT (EMPTY, (0 0))",
            "LINESTRING (0 0, 1 1)",
            "MULTILINESTRING (EMPTY, (0 0, 1 1))",
            "POLYGON ((0 0, 2 0, 2 2, 0 0), (0.5 0.5, 1 0.5, 1 1, 0.5 0.5))",
            "MULTIPOLYGON (EMPTY, ((0 0, 2 0, 2 2, 0 0)))",
            "GEOMETRYCOLLECTION (POINT (0 0), GEOMETRYCOLLECTION (POINT EMPTY))",
        ] {
            let shape = crate::io::parse_wkt(source).expect("fixture WKT parses");
            let output = to_wkt(&shape).expect("fixture WKT serializes");
            let summary = wkt_text_summary(&shape);
            let upper_bound = summary
                .wkt_upper_bound(
                    shape.axes().dimension(),
                    WktNumberFormat::Shortest.maximum_ordinate_bytes(),
                )
                .expect("fixture upper bound is representable");
            assert!(
                output.len() <= upper_bound,
                "upper bound {upper_bound} is below output length {} for {source}",
                output.len()
            );
        }
    }

    #[test]
    fn checked_writer_handles_upper_bound_exceeding_a_fitting_lower_bound() {
        let small_shape = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let small_limit = 20;
        let small_summary = wkt_text_summary(&small_shape);
        assert!(matches!(
            small_summary.wkt_lower_bound(small_limit),
            TextWidthBound::Within { lower_bound, .. } if lower_bound <= small_limit
        ));
        assert!(
            small_summary
                .wkt_upper_bound(
                    small_shape.axes().dimension(),
                    WktNumberFormat::Shortest.maximum_ordinate_bytes(),
                )
                .is_some_and(|upper_bound| upper_bound > small_limit)
        );
        assert_eq!(
            format_wkt_with_limit(&small_shape, WktNumberFormat::Shortest, small_limit,)
                .expect("checked fallback can emit bytes below the limit"),
            "POINT (0 0)"
        );

        let shape = Shape::Point(Point::new_raw_xy(f64::MAX, 0.0));
        let limit = 100;
        let summary = wkt_text_summary(&shape);
        assert!(matches!(
            summary.wkt_lower_bound(limit),
            TextWidthBound::Within { lower_bound, .. } if lower_bound <= limit
        ));
        assert!(
            summary
                .wkt_upper_bound(
                    shape.axes().dimension(),
                    WktNumberFormat::Fixed(255).maximum_ordinate_bytes(),
                )
                .is_some_and(|upper_bound| upper_bound > limit)
        );

        let error = format_wkt_with_limit(&shape, WktNumberFormat::Fixed(255), limit)
            .expect_err("fixed-width WKT exceeds the output limit");
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn checked_wkt_accepts_exact_byte_limit_and_rejects_one_byte_less() {
        let shape = Shape::Point(Point::new_raw_xy(12.34, 56.78));
        let expected = "POINT (12.34 56.78)";
        assert_eq!(
            format_wkt_with_limit(&shape, WktNumberFormat::Shortest, expected.len())
                .expect("exact output width fits"),
            expected
        );
        let error = format_wkt_with_limit(&shape, WktNumberFormat::Shortest, expected.len() - 1)
            .expect_err("one byte less than the output width fails");
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn checked_wkt_trims_precision_255_before_charging_output_bytes() {
        let shape = Shape::Point(Point::new_raw_xy(1.0, 2.0));
        let expected = "POINT (1 2)";
        assert_eq!(expected.len(), 11);
        assert_eq!(
            format_wkt_with_limit(&shape, WktNumberFormat::Trimmed(255), expected.len())
                .expect("trimmed numbers fit their final byte budget"),
            expected
        );
        let error =
            format_wkt_with_limit(&shape, WktNumberFormat::Trimmed(255), expected.len() - 1)
                .expect_err("one byte less than the trimmed output width fails");
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn wkt_preview_keeps_the_complete_document_prefix_at_nested_boundaries() {
        let shape = crate::io::parse_wkt(
            "GEOMETRYCOLLECTION (LINESTRING (12.34 56.78, 90.12 34.56), POINT (7.89 0.12))",
        )
        .expect("fixture WKT parses");
        let expected =
            "GEOMETRYCOLLECTION (LINESTRING (12.34 56.78, 90.12 34.56), POINT (7.89 0.12))";
        assert_eq!(to_wkt(&shape).expect("fixture WKT serializes"), expected);

        for limit in [1, 19, 20, 21, 31, 32, 35, 43, 48, 54, 60] {
            let preview = to_wkt_preview(&shape, limit).expect("prefix preview serializes");
            assert!(preview.len() >= limit, "limit={limit}, preview={preview}");
            assert_eq!(
                &preview.as_bytes()[..limit],
                &expected.as_bytes()[..limit],
                "limit={limit}"
            );
        }
    }

    #[test]
    fn ewkt_body_axes_error_precedes_invalid_srid_prefix() {
        let xyz = |x: f64, y: f64| {
            Point::new_axes(x, y, ZOrdinate(Some(3.0)), MOrdinate(None)).expect("finite XYZ point")
        };
        let xy = |x: f64, y: f64| Point::new(x, y).expect("finite XY point");
        let shell = Ring::from_closed(
            CoordSeq::try_from_points(&[
                xyz(0.0, 0.0),
                xyz(2.0, 0.0),
                xyz(0.0, 2.0),
                xyz(0.0, 0.0),
            ])
            .expect("closed XYZ shell"),
        )
        .expect("valid XYZ shell");
        let hole = Ring::from_closed(
            CoordSeq::try_from_points(&[xy(0.5, 0.5), xy(1.0, 0.5), xy(0.5, 1.0), xy(0.5, 0.5)])
                .expect("closed XY hole"),
        )
        .expect("valid XY hole");
        let shape = Shape::Polygon(Polygon::new(shell, vec![hole]));

        let error = to_wkt_with_srid(&shape, Some("not a CRS"), true)
            .expect_err("body axes fail before SRID prefix resolution");
        assert!(matches!(error.kind(), ErrorKind::Geometry(_)));
    }

    #[test]
    fn ewkt_valid_body_reaches_srid_validation_and_keeps_exact_prefix() {
        let point = Shape::Point(Point::new(1.0, 2.0).expect("finite point"));
        let error = to_wkt_with_srid(&point, Some("not a CRS"), true)
            .expect_err("a valid body proceeds to SRID validation");
        assert!(matches!(error.kind(), ErrorKind::Crs(_)));
        assert_eq!(
            to_wkt_with_srid(&point, Some("EPSG:4326"), true).expect("EPSG SRID is supported"),
            "SRID=4326;POINT (1 2)"
        );
    }
}
