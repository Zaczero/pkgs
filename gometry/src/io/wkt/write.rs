use crate::geometry::GeometryErrorKind;
use crate::io::wkt::{
    CoordinateAxes, Coordinates, GeometryKind, Point, Polygon, Result, Shape, extended_srid_code,
    require_serializable_axes,
};
use crate::io::{CheckedText, wkt_text_summary};

#[derive(Default)]
struct WktScratch {
    number: String,
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
    // Hold the SRID result so body errors keep their historical precedence.
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
                // `zmij` produces the same unique shortest-round-trip digits
                // as `std` Display but faster; it switches to exponent
                // notation outside this range, where `std` (always plain)
                // keeps the WKT contract.
                if value == 0.0 || (1e-5..1e16).contains(&value.abs()) {
                    let mut buffer = zmij::Buffer::new();
                    let digits = buffer.format_finite(value);
                    // std omits the ".0" on integral values; zmij keeps it.
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
                // Format into bounded per-number scratch before checking the
                // trimmed bytes against the output limit.
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
            // Display is plain decimal: a binary64's longest exact decimal
            // expansion stays below 1,100 bytes including its sign and point.
            Self::Shortest => 1_100,
            Self::Fixed(precision) | Self::Trimmed(precision) => {
                usize::from(precision).saturating_add(311)
            },
        }
    }
}

/// WKT with display-formatted ordinates (see [`WktNumberFormat`]); backs
/// `Geometry.__format__`.
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

    // Reserve for ordinary decimal spelling; extreme plain floats and long
    // fixed representations grow normally instead of retaining their
    // worst-case capacity in every row result.
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
        let _reservation = out.try_reserve(capacity);
        write_wkt(&mut out, shape, format, usize::MAX)?;
        return Ok(out);
    }

    let mut out = CheckedText::with_capacity_hint(capacity.min(limit), limit);
    let mut scratch = WktScratch::default();
    write_wkt_checked(&mut out, shape, format, usize::MAX, &mut scratch)?;
    Ok(out.into_string())
}

/// WKT rendered only far enough to fill `limit` bytes.
///
/// The leading prefix of the full [`to_wkt`] render, for previews (`repr`,
/// error messages) that truncate anyway — a huge geometry pays for ~`limit`
/// bytes, not its whole WKT.
pub(crate) fn to_wkt_preview(shape: &Shape, limit: usize) -> Result<String> {
    let capacity = limit
        .checked_add(4096)
        .filter(|capacity| isize::try_from(*capacity).is_ok())
        .unwrap_or_default();
    let mut out = String::new();
    let _reservation = out.try_reserve(capacity);
    write_wkt(&mut out, shape, WktNumberFormat::Shortest, limit)?;
    Ok(out)
}

/// Streaming WKT writer with an output byte budget: emission stops once `out`
/// reaches `limit` bytes (pass [`usize::MAX`] for the unbounded full render).
/// Bounded callers — `Geometry.__repr__`, error-message previews — pay for only
/// the leading ~`limit` bytes instead of materializing a megabyte of WKT to
/// truncate it; the emitted prefix is byte-identical to the full render's, so a
/// later `truncate(limit)` yields exactly the same text.
fn write_wkt_checked(
    out: &mut CheckedText,
    shape: &Shape,
    format: WktNumberFormat,
    limit: usize,
    scratch: &mut WktScratch,
) -> Result<()> {
    match shape {
        Shape::Point(point) => {
            let axes = CoordinateAxes::from_point(*point);
            out.write_formatted(format_args!("POINT{} (", axes.wkt_tag()))?;
            write_wkt_point_checked(out, *point, axes, format, scratch)?;
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
                        write_wkt_point_checked(
                            out,
                            point,
                            CoordinateAxes::from_point(point),
                            format,
                            scratch,
                        )?;
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
            write_wkt_ring_checked(out, points, axes, format, limit, scratch)?;
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
                        write_wkt_ring_checked(out, line, axes, format, limit, scratch)?;
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
            write_wkt_polygon_checked(out, polygon, axes, format, limit, scratch)?;
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
                        write_wkt_polygon_checked(out, polygon, axes, format, limit, scratch)?;
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
                write_wkt_checked(out, geometry, format, limit, scratch)?;
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
            write_wkt_empty_checked(out, keyword, *axes)?;
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

/// `{KEYWORD}{ Z| M| ZM} EMPTY` — the dimensional tag survives emptiness.
fn write_wkt_empty_checked(
    out: &mut CheckedText,
    keyword: &str,
    axes: CoordinateAxes,
) -> Result<()> {
    out.push_str(keyword)?;
    out.push_str(axes.wkt_tag())?;
    out.push_str(" EMPTY")
}

fn write_wkt_ring_checked<C: Coordinates + ?Sized>(
    out: &mut CheckedText,
    points: &C,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
    scratch: &mut WktScratch,
) -> Result<()> {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        write_wkt_ring_impl_checked::<HAS_Z, HAS_M, C>(out, points, format, limit, scratch)
    })
}

fn write_wkt_ring_impl_checked<const Z: bool, const M: bool, C: Coordinates + ?Sized>(
    out: &mut CheckedText,
    points: &C,
    format: WktNumberFormat,
    limit: usize,
    scratch: &mut WktScratch,
) -> Result<()> {
    if let Some((xs, ys)) = points.xy_columns() {
        if Z {
            let zs = points.z_column().expect("WKT Z axis was preflighted");
            if M {
                let ms = points.m_column().expect("WKT M axis was preflighted");
                return write_wkt_column_rows_checked::<Z, M, _>(
                    out,
                    format,
                    limit,
                    xs.iter()
                        .copied()
                        .zip(ys.iter().copied())
                        .zip(zs.iter().copied())
                        .zip(ms.iter().copied())
                        .map(|(((x, y), z), m)| (x, y, z, m)),
                    scratch,
                );
            }
            return write_wkt_column_rows_checked::<Z, M, _>(
                out,
                format,
                limit,
                xs.iter()
                    .copied()
                    .zip(ys.iter().copied())
                    .zip(zs.iter().copied())
                    .map(|((x, y), z)| (x, y, z, 0.0)),
                scratch,
            );
        }
        if M {
            let ms = points.m_column().expect("WKT M axis was preflighted");
            return write_wkt_column_rows_checked::<Z, M, _>(
                out,
                format,
                limit,
                xs.iter()
                    .copied()
                    .zip(ys.iter().copied())
                    .zip(ms.iter().copied())
                    .map(|((x, y), m)| (x, y, 0.0, m)),
                scratch,
            );
        }
        return write_wkt_column_rows_checked::<Z, M, _>(
            out,
            format,
            limit,
            xs.iter()
                .copied()
                .zip(ys.iter().copied())
                .map(|(x, y)| (x, y, 0.0, 0.0)),
            scratch,
        );
    }

    for (index, point) in points.iter_coords().enumerate() {
        if out.len() >= limit {
            break;
        }
        if index > 0 {
            out.push_str(", ")?;
        }
        write_wkt_point_impl_checked::<Z, M>(out, point, format, scratch)?;
    }
    Ok(())
}

fn write_wkt_column_rows_checked<const Z: bool, const M: bool, I>(
    out: &mut CheckedText,
    format: WktNumberFormat,
    limit: usize,
    rows: I,
    scratch: &mut WktScratch,
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
        write_wkt_coordinate_impl_checked::<Z, M>(out, x, y, z, m, format, scratch)?;
    }
    Ok(())
}

fn write_wkt_polygon_checked(
    out: &mut CheckedText,
    polygon: &Polygon,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
    scratch: &mut WktScratch,
) -> Result<()> {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        write_wkt_polygon_impl_checked::<HAS_Z, HAS_M>(out, polygon, axes, format, limit, scratch)
    })
}

fn write_wkt_polygon_impl_checked<const Z: bool, const M: bool>(
    out: &mut CheckedText,
    polygon: &Polygon,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
    scratch: &mut WktScratch,
) -> Result<()> {
    require_wkt_axes(polygon.shell.axes(), axes)?;
    out.push('(')?;
    write_wkt_ring_impl_checked::<Z, M, _>(out, &polygon.shell, format, limit, scratch)?;
    out.push(')')?;
    for hole in polygon.holes.iter() {
        if out.len() >= limit {
            break;
        }
        require_wkt_axes(hole.axes(), axes)?;
        out.push_str(", (")?;
        write_wkt_ring_impl_checked::<Z, M, _>(out, hole, format, limit, scratch)?;
        out.push(')')?;
    }
    Ok(())
}

fn write_wkt_point_checked(
    out: &mut CheckedText,
    point: Point,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    scratch: &mut WktScratch,
) -> Result<()> {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        write_wkt_point_impl_checked::<HAS_Z, HAS_M>(out, point, format, scratch)
    })
}

fn write_wkt_point_impl_checked<const Z: bool, const M: bool>(
    out: &mut CheckedText,
    point: Point,
    format: WktNumberFormat,
    scratch: &mut WktScratch,
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
    write_wkt_coordinate_impl_checked::<Z, M>(out, point.x, point.y, z, m, format, scratch)
}

fn write_wkt_coordinate_impl_checked<const Z: bool, const M: bool>(
    out: &mut CheckedText,
    x: f64,
    y: f64,
    z: f64,
    m: f64,
    format: WktNumberFormat,
    scratch: &mut WktScratch,
) -> Result<()> {
    let WktScratch { number } = scratch;
    format.write_checked(out, number, x)?;
    out.push(' ')?;
    format.write_checked(out, number, y)?;
    if Z {
        out.push(' ')?;
        format.write_checked(out, number, z)?;
    }
    if M {
        out.push(' ')?;
        format.write_checked(out, number, m)?;
    }
    Ok(())
}

/// Fast WKT writer used only when the owner-aware upper bound proves that all
/// bytes fit in the destination `String`.
fn write_wkt(out: &mut String, shape: &Shape, format: WktNumberFormat, limit: usize) -> Result<()> {
    use std::fmt::Write as _;
    match shape {
        Shape::Point(point) => {
            let axes = CoordinateAxes::from_point(*point);
            write!(out, "POINT{} (", axes.wkt_tag()).expect("writing WKT to String");
            write_wkt_point(out, *point, axes, format);
            out.push(')');
            Ok(())
        },
        Shape::MultiPoint(points) => {
            write!(out, "MULTIPOINT{} (", shape.axes().wkt_tag()).expect("writing WKT to String");
            for (index, member) in points.members().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ");
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY"),
                    crate::geometry::MultiMember::Value(point_index) => {
                        let point = points.point_at(point_index);
                        out.push('(');
                        write_wkt_point(out, point, CoordinateAxes::from_point(point), format);
                        out.push(')');
                    },
                }
            }
            out.push(')');
            Ok(())
        },
        Shape::LineString(points) => {
            let axes = shape.axes();
            write!(out, "LINESTRING{} (", axes.wkt_tag()).expect("writing WKT to String");
            write_wkt_ring(out, points, axes, format, limit);
            out.push(')');
            Ok(())
        },
        Shape::MultiLineString(lines) => {
            let axes = shape.axes();
            write!(out, "MULTILINESTRING{} (", axes.wkt_tag()).expect("writing WKT to String");
            for (index, member) in lines.members_with_values().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ");
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY"),
                    crate::geometry::MultiMember::Value(line) => {
                        require_wkt_axes(line.axes(), axes)?;
                        out.push('(');
                        write_wkt_ring(out, line, axes, format, limit);
                        out.push(')');
                    },
                }
            }
            out.push(')');
            Ok(())
        },
        Shape::Polygon(polygon) => {
            let axes = shape.axes();
            write!(out, "POLYGON{} (", axes.wkt_tag()).expect("writing WKT to String");
            write_wkt_polygon(out, polygon, axes, format, limit)?;
            out.push(')');
            Ok(())
        },
        Shape::MultiPolygon(polygons) => {
            let axes = shape.axes();
            write!(out, "MULTIPOLYGON{} (", axes.wkt_tag()).expect("writing WKT to String");
            for (index, member) in polygons.members_with_values().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ");
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY"),
                    crate::geometry::MultiMember::Value(polygon) => {
                        out.push('(');
                        write_wkt_polygon(out, polygon, axes, format, limit)?;
                        out.push(')');
                    },
                }
            }
            out.push(')');
            Ok(())
        },
        Shape::GeometryCollection(geometries) => {
            out.push_str("GEOMETRYCOLLECTION (");
            for (index, geometry) in geometries.iter().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ");
                }
                write_wkt(out, geometry, format, limit)?;
            }
            out.push(')');
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
            write_wkt_empty(out, keyword, *axes);
            Ok(())
        },
    }
}

fn write_wkt_empty(out: &mut String, keyword: &str, axes: CoordinateAxes) {
    out.push_str(keyword);
    out.push_str(axes.wkt_tag());
    out.push_str(" EMPTY");
}

fn write_wkt_ring<C: Coordinates + ?Sized>(
    out: &mut String,
    points: &C,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
) {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        write_wkt_ring_impl::<HAS_Z, HAS_M, C>(out, points, format, limit);
    });
}

fn write_wkt_ring_impl<const Z: bool, const M: bool, C: Coordinates + ?Sized>(
    out: &mut String,
    points: &C,
    format: WktNumberFormat,
    limit: usize,
) {
    if let Some((xs, ys)) = points.xy_columns() {
        if Z {
            let zs = points.z_column().expect("WKT Z axis was preflighted");
            if M {
                let ms = points.m_column().expect("WKT M axis was preflighted");
                write_wkt_column_rows::<Z, M, _>(
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
            } else {
                write_wkt_column_rows::<Z, M, _>(
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
        } else if M {
            let ms = points.m_column().expect("WKT M axis was preflighted");
            write_wkt_column_rows::<Z, M, _>(
                out,
                format,
                limit,
                xs.iter()
                    .copied()
                    .zip(ys.iter().copied())
                    .zip(ms.iter().copied())
                    .map(|((x, y), m)| (x, y, 0.0, m)),
            );
        } else {
            write_wkt_column_rows::<Z, M, _>(
                out,
                format,
                limit,
                xs.iter()
                    .copied()
                    .zip(ys.iter().copied())
                    .map(|(x, y)| (x, y, 0.0, 0.0)),
            );
        }
        return;
    }

    for (index, point) in points.iter_coords().enumerate() {
        if out.len() >= limit {
            break;
        }
        if index > 0 {
            out.push_str(", ");
        }
        write_wkt_point_impl::<Z, M>(out, point, format);
    }
}

fn write_wkt_column_rows<const Z: bool, const M: bool, I>(
    out: &mut String,
    format: WktNumberFormat,
    limit: usize,
    rows: I,
) where
    I: Iterator<Item = (f64, f64, f64, f64)>,
{
    for (index, (x, y, z, m)) in rows.enumerate() {
        if out.len() >= limit {
            break;
        }
        if index > 0 {
            out.push_str(", ");
        }
        write_wkt_coordinate_impl::<Z, M>(out, x, y, z, m, format);
    }
}

fn write_wkt_polygon(
    out: &mut String,
    polygon: &Polygon,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
) -> Result<()> {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        write_wkt_polygon_impl::<HAS_Z, HAS_M>(out, polygon, axes, format, limit)
    })
}

fn write_wkt_polygon_impl<const Z: bool, const M: bool>(
    out: &mut String,
    polygon: &Polygon,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
) -> Result<()> {
    require_wkt_axes(polygon.shell.axes(), axes)?;
    out.push('(');
    write_wkt_ring_impl::<Z, M, _>(out, &polygon.shell, format, limit);
    out.push(')');
    for hole in polygon.holes.iter() {
        if out.len() >= limit {
            break;
        }
        require_wkt_axes(hole.axes(), axes)?;
        out.push_str(", (");
        write_wkt_ring_impl::<Z, M, _>(out, hole, format, limit);
        out.push(')');
    }
    Ok(())
}

fn write_wkt_point(out: &mut String, point: Point, axes: CoordinateAxes, format: WktNumberFormat) {
    crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
        write_wkt_point_impl::<HAS_Z, HAS_M>(out, point, format);
    });
}

fn write_wkt_point_impl<const Z: bool, const M: bool>(
    out: &mut String,
    point: Point,
    format: WktNumberFormat,
) {
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
    write_wkt_coordinate_impl::<Z, M>(out, point.x, point.y, z, m, format);
}

fn write_wkt_coordinate_impl<const Z: bool, const M: bool>(
    out: &mut String,
    x: f64,
    y: f64,
    z: f64,
    m: f64,
    format: WktNumberFormat,
) {
    format.write(out, x);
    out.push(' ');
    format.write(out, y);
    if Z {
        out.push(' ');
        format.write(out, z);
    }
    if M {
        out.push(' ');
        format.write(out, m);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::io::TextWidthBound;

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
}
