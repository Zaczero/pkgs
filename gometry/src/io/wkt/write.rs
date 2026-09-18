use crate::geometry::GeometryErrorKind;
use crate::io::text_structure_count;
use crate::io::wkt::{
    CoordinateAxes, Coordinates, GeometryKind, IoError, Point, Polygon, Result, Shape,
    extended_srid_code, require_serializable_axes,
};

pub(crate) fn to_wkt(shape: &Shape) -> Result<String> {
    require_serializable_axes(shape)?;
    format_wkt(shape, None, WktNumberFormat::Shortest)
}

fn wkt_srid_prefix(crs: Option<&str>, include_srid: bool) -> Result<Option<String>> {
    extended_srid_code(crs, include_srid, "EWKT")
        .map(|code| code.map(|code| format!("SRID={code};")))
}

pub(crate) fn to_wkt_with_dimension(
    shape: &Shape,
    output_dimension: Option<WktDimension>,
    crs: Option<&str>,
    include_srid: bool,
) -> Result<String> {
    // Reject heterogeneous-axis multiparts before inventing missing Z/M as 0.
    require_serializable_axes(shape)?;
    let body = format_wkt(shape, output_dimension, WktNumberFormat::Shortest)?;
    match wkt_srid_prefix(crs, include_srid)? {
        Some(prefix) => {
            let mut output = String::new();
            let length = prefix
                .len()
                .checked_add(body.len())
                .ok_or_else(|| IoError::wkt("WKT output length overflows usize"))?;
            output
                .try_reserve(length)
                .map_err(crate::error::Error::from)?;
            output.push_str(&prefix);
            output.push_str(&body);
            Ok(output)
        },
        None => Ok(body),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WktDimension {
    Two,
    Three,
    Four,
}

impl TryFrom<u8> for WktDimension {
    type Error = crate::error::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(Self::Two),
            3 => Ok(Self::Three),
            4 => Ok(Self::Four),
            _ => Err(IoError::wkt(format!(
                "WKT output_dimension must be 2, 3, or 4, got {value}"
            ))),
        }
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
                    let _ = write!(out, "{value}");
                }
            },
            Self::Fixed(precision) | Self::Trimmed(precision) => {
                let start = out.len();
                let _ = write!(out, "{value:.*}", usize::from(precision));
                if matches!(self, Self::Trimmed(_))
                    && let Some(rendered) = out.get(start..)
                    && rendered.contains('.')
                {
                    let keep = rendered.trim_end_matches('0').trim_end_matches('.').len();
                    out.truncate(start + keep);
                }
            },
        }
    }
}

/// WKT with display-formatted ordinates (see [`WktNumberFormat`]); backs
/// `Geometry.__format__`.
pub(crate) fn to_wkt_display(shape: &Shape, format: WktNumberFormat) -> Result<String> {
    require_serializable_axes(shape)?;
    format_wkt(shape, None, format)
}

fn format_wkt(
    shape: &Shape,
    output_dimension: Option<WktDimension>,
    format: WktNumberFormat,
) -> Result<String> {
    // Four shortest plain f64 values fit within 1,315 bytes, while four
    // precision-255 fixed values fit within 2,271 bytes. Structural tokens
    // cover the remaining delimiters, including empty nested geometries.
    let coordinate_bytes_per_coord = match format {
        WktNumberFormat::Shortest => 1344,
        WktNumberFormat::Fixed(_) | WktNumberFormat::Trimmed(_) => 2304,
    };
    let coordinate_bytes = shape
        .coord_count()
        .checked_mul(coordinate_bytes_per_coord)
        .ok_or_else(|| IoError::wkt("WKT coordinate byte length overflows usize"))?;
    let structural_bytes = text_structure_count(shape)
        .and_then(|count| count.checked_mul(128))
        .ok_or_else(|| IoError::wkt("WKT structural byte length overflows usize"))?;
    let capacity = coordinate_bytes
        .checked_add(structural_bytes)
        .ok_or_else(|| IoError::wkt("WKT output length overflows usize"))?;
    let mut out = String::new();
    out.try_reserve(capacity)
        .map_err(crate::error::Error::from)?;
    write_wkt(&mut out, shape, output_dimension, format, usize::MAX)?;
    Ok(out)
}

/// WKT rendered only far enough to fill `limit` bytes.
///
/// The leading prefix of the full [`to_wkt`] render, for previews (`repr`,
/// error messages) that truncate anyway — a huge geometry pays for ~`limit`
/// bytes, not its whole WKT.
pub(crate) fn to_wkt_preview(shape: &Shape, limit: usize) -> Result<String> {
    let capacity = limit
        .checked_add(4096)
        .ok_or_else(|| IoError::wkt("WKT preview length overflows usize"))?;
    let mut out = String::new();
    out.try_reserve(capacity)
        .map_err(crate::error::Error::from)?;
    write_wkt(&mut out, shape, None, WktNumberFormat::Shortest, limit)?;
    Ok(out)
}

/// Streaming WKT writer: appends `shape` to `out` with no intermediate
/// per-coordinate/per-ring `String`s. With [`WktNumberFormat::Shortest`] the
/// text matches the historical `Display` rendering byte-for-byte; the other
/// formats only change how each ordinate is printed.
/// Streaming WKT writer with an output byte budget: emission stops once `out`
/// reaches `limit` bytes (pass [`usize::MAX`] for the unbounded full render).
/// Bounded callers — `Geometry.__repr__`, error-message previews — pay for only
/// the leading ~`limit` bytes instead of materializing a megabyte of WKT to
/// truncate it; the emitted prefix is byte-identical to the full render's, so a
/// later `truncate(limit)` yields exactly the same text.
fn write_wkt(
    out: &mut String,
    shape: &Shape,
    output_dimension: Option<WktDimension>,
    format: WktNumberFormat,
    limit: usize,
) -> Result<()> {
    use std::fmt::Write as _;
    match shape {
        Shape::Point(point) => {
            let axes = wkt_output_axes(CoordinateAxes::from_point(*point), output_dimension);
            let _ = write!(out, "POINT{} (", axes.wkt_tag());
            write_wkt_point(out, *point, axes, format);
            out.push(')');
            Ok(())
        },
        Shape::MultiPoint(points) => {
            let _ = write!(
                out,
                "MULTIPOINT{} (",
                wkt_output_axes(shape.axes(), output_dimension).wkt_tag()
            );
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
                        write_wkt_point(
                            out,
                            point,
                            wkt_output_axes(CoordinateAxes::from_point(point), output_dimension),
                            format,
                        );
                        out.push(')');
                    },
                }
            }
            out.push(')');
            Ok(())
        },
        Shape::LineString(points) => {
            let axes = wkt_output_axes(shape.axes(), output_dimension);
            let _ = write!(out, "LINESTRING{} (", axes.wkt_tag());
            write_wkt_ring(out, points, axes, format, limit);
            out.push(')');
            Ok(())
        },
        Shape::MultiLineString(lines) => {
            let axes = wkt_output_axes(shape.axes(), output_dimension);
            let _ = write!(out, "MULTILINESTRING{} (", axes.wkt_tag());
            for (index, member) in lines.members().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ");
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY"),
                    crate::geometry::MultiMember::Value(line_index) => {
                        let line = &lines.as_slice()[line_index];
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
            let axes = wkt_output_axes(shape.axes(), output_dimension);
            let _ = write!(out, "POLYGON{} (", axes.wkt_tag());
            write_wkt_polygon(out, polygon, axes, format, limit)?;
            out.push(')');
            Ok(())
        },
        Shape::MultiPolygon(polygons) => {
            let axes = wkt_output_axes(shape.axes(), output_dimension);
            let _ = write!(out, "MULTIPOLYGON{} (", axes.wkt_tag());
            for (index, member) in polygons.members().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ");
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY"),
                    crate::geometry::MultiMember::Value(polygon_index) => {
                        out.push('(');
                        write_wkt_polygon(
                            out,
                            &polygons.as_slice()[polygon_index],
                            axes,
                            format,
                            limit,
                        )?;
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
                write_wkt(out, geometry, output_dimension, format, limit)?;
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
            write_wkt_empty(out, keyword, *axes, output_dimension);
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

/// `{KEYWORD}{ Z| M| ZM} EMPTY` — the dimensional tag survives emptiness,
/// filtered through the same `output_dimension` policy as coordinates.
fn write_wkt_empty(
    out: &mut String,
    keyword: &str,
    axes: CoordinateAxes,
    output_dimension: Option<WktDimension>,
) {
    out.push_str(keyword);
    out.push_str(wkt_output_axes(axes, output_dimension).wkt_tag());
    out.push_str(" EMPTY");
}

fn write_wkt_ring<C: Coordinates + ?Sized>(
    out: &mut String,
    points: &C,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
) {
    match (axes.has_z(), axes.has_m()) {
        (false, false) => write_wkt_ring_impl::<false, false, C>(out, points, format, limit),
        (true, false) => write_wkt_ring_impl::<true, false, C>(out, points, format, limit),
        (false, true) => write_wkt_ring_impl::<false, true, C>(out, points, format, limit),
        (true, true) => write_wkt_ring_impl::<true, true, C>(out, points, format, limit),
    }
}

fn write_wkt_ring_impl<const Z: bool, const M: bool, C: Coordinates + ?Sized>(
    out: &mut String,
    points: &C,
    format: WktNumberFormat,
    limit: usize,
) {
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

fn write_wkt_polygon(
    out: &mut String,
    polygon: &Polygon,
    axes: CoordinateAxes,
    format: WktNumberFormat,
    limit: usize,
) -> Result<()> {
    match (axes.has_z(), axes.has_m()) {
        (false, false) => write_wkt_polygon_impl::<false, false>(out, polygon, axes, format, limit),
        (true, false) => write_wkt_polygon_impl::<true, false>(out, polygon, axes, format, limit),
        (false, true) => write_wkt_polygon_impl::<false, true>(out, polygon, axes, format, limit),
        (true, true) => write_wkt_polygon_impl::<true, true>(out, polygon, axes, format, limit),
    }
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
    match (axes.has_z(), axes.has_m()) {
        (false, false) => write_wkt_point_impl::<false, false>(out, point, format),
        (true, false) => write_wkt_point_impl::<true, false>(out, point, format),
        (false, true) => write_wkt_point_impl::<false, true>(out, point, format),
        (true, true) => write_wkt_point_impl::<true, true>(out, point, format),
    }
}

fn write_wkt_point_impl<const Z: bool, const M: bool>(
    out: &mut String,
    point: Point,
    format: WktNumberFormat,
) {
    format.write(out, point.x);
    out.push(' ');
    format.write(out, point.y);
    if Z {
        out.push(' ');
        format.write(out, point.z().expect("WKT Z axis was preflighted"));
    }
    if M {
        out.push(' ');
        format.write(out, point.m().expect("WKT M axis was preflighted"));
    }
}

const fn wkt_output_axes(
    axes: CoordinateAxes,
    output_dimension: Option<WktDimension>,
) -> CoordinateAxes {
    match output_dimension {
        Some(WktDimension::Two) => CoordinateAxes::XY,
        // 3D output keeps Z when present (dropping M), else passes XYM/XY through.
        Some(WktDimension::Three) if axes.has_z() => CoordinateAxes::XYZ,
        None | Some(WktDimension::Three | WktDimension::Four) => axes,
    }
}
