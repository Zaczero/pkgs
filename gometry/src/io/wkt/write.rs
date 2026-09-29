use crate::geometry::{GeometryErrorKind, ShapeCountQuery};
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
            let length = prefix.len().wrapping_add(body.len());
            let mut output = String::with_capacity(length);
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
    // Reserve for ordinary decimal spelling; extreme plain floats and long
    // fixed representations grow normally instead of retaining their
    // worst-case capacity in every row result.
    let bytes_per_ordinate = match format {
        WktNumberFormat::Shortest => 24,
        WktNumberFormat::Fixed(precision) | WktNumberFormat::Trimmed(precision) => {
            usize::from(precision).wrapping_add(24)
        },
    };
    let coordinate_bytes_per_coord = wkt_output_axes(shape.axes(), output_dimension)
        .dimension()
        .checked_mul(bytes_per_ordinate);
    let capacity = ShapeCountQuery::default()
        .coordinates(shape)
        .ok()
        .zip(coordinate_bytes_per_coord)
        .and_then(|(count, bytes_per_coord)| count.checked_mul(bytes_per_coord))
        .zip(text_structure_count(shape).checked_mul(32))
        .and_then(|(coordinate_bytes, structural_bytes)| {
            coordinate_bytes.checked_add(structural_bytes)
        })
        .unwrap_or_default();
    let mut out = String::new();
    let _reservation = out.try_reserve(capacity);
    write_wkt(&mut out, shape, output_dimension, format, usize::MAX)?;
    Ok(out)
}

/// WKT rendered only far enough to fill `limit` bytes.
///
/// The leading prefix of the full [`to_wkt`] render, for previews (`repr`,
/// error messages) that truncate anyway — a huge geometry pays for ~`limit`
/// bytes, not its whole WKT.
pub(crate) fn to_wkt_preview(shape: &Shape, limit: usize) -> Result<String> {
    let capacity = limit.wrapping_add(4096);
    let mut out = String::with_capacity(capacity);
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
            write!(out, "POINT{} (", axes.wkt_tag()).expect("writing WKT to String");
            write_wkt_point(out, *point, axes, format);
            out.push(')');
            Ok(())
        },
        Shape::MultiPoint(points) => {
            write!(
                out,
                "MULTIPOINT{} (",
                wkt_output_axes(shape.axes(), output_dimension).wkt_tag()
            )
            .expect("writing WKT to String");
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
            write!(out, "LINESTRING{} (", axes.wkt_tag()).expect("writing WKT to String");
            write_wkt_ring(out, points, axes, format, limit);
            out.push(')');
            Ok(())
        },
        Shape::MultiLineString(lines) => {
            let axes = wkt_output_axes(shape.axes(), output_dimension);
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
            let axes = wkt_output_axes(shape.axes(), output_dimension);
            write!(out, "POLYGON{} (", axes.wkt_tag()).expect("writing WKT to String");
            write_wkt_polygon(out, polygon, axes, format, limit)?;
            out.push(')');
            Ok(())
        },
        Shape::MultiPolygon(polygons) => {
            let axes = wkt_output_axes(shape.axes(), output_dimension);
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
