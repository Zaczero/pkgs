use crate::io::wkt::{
    CoordinateAxes, Coordinates, GeometryKind, IoError, Point, Polygon, Result, Shape,
    extended_srid_code, require_serializable_axes,
};

pub(crate) fn to_wkt(shape: &Shape) -> Result<String> {
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
                .map_err(|_| crate::error::ResourceError::Allocation)?;
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
            Self::Fixed(precision) => {
                let _ = write!(out, "{value:.*}", usize::from(precision));
            },
            Self::Trimmed(precision) => {
                let start = out.len();
                let _ = write!(out, "{value:.*}", usize::from(precision));
                if let Some(rendered) = out.get(start..)
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
    format_wkt(shape, None, format)
}

fn format_wkt(
    shape: &Shape,
    output_dimension: Option<WktDimension>,
    format: WktNumberFormat,
) -> Result<String> {
    // Reserve enough for the largest supported fixed-format f64 representation
    // and for structural tokens, including empty nested geometries.
    let coordinate_bytes = shape
        .coord_count()
        .checked_mul(4096)
        .ok_or_else(|| IoError::wkt("WKT coordinate byte length overflows usize"))?;
    let structural_bytes = wkt_structure_count(shape)?
        .checked_mul(128)
        .ok_or_else(|| IoError::wkt("WKT structural byte length overflows usize"))?;
    let capacity = coordinate_bytes
        .checked_add(structural_bytes)
        .ok_or_else(|| IoError::wkt("WKT output length overflows usize"))?;
    let mut out = String::new();
    out.try_reserve(capacity)
        .map_err(|_| crate::error::ResourceError::Allocation)?;
    write_wkt(&mut out, shape, output_dimension, format, usize::MAX);
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
        .map_err(|_| crate::error::ResourceError::Allocation)?;
    write_wkt(&mut out, shape, None, WktNumberFormat::Shortest, limit);
    Ok(out)
}

fn wkt_structure_count(shape: &Shape) -> Result<usize> {
    match shape {
        Shape::MultiLineString(lines) => lines
            .len()
            .checked_add(1)
            .ok_or_else(|| IoError::wkt("WKT structural count overflows usize")),
        Shape::Polygon(polygon) => polygon
            .holes
            .len()
            .checked_add(1)
            .ok_or_else(|| IoError::wkt("WKT structural count overflows usize")),
        Shape::MultiPolygon(polygons) => polygons.iter().try_fold(1_usize, |total, polygon| {
            let polygon_count = polygon
                .holes
                .len()
                .checked_add(1)
                .ok_or_else(|| IoError::wkt("WKT structural count overflows usize"))?;
            total
                .checked_add(polygon_count)
                .ok_or_else(|| IoError::wkt("WKT structural count overflows usize"))
        }),
        Shape::GeometryCollection(geometries) => {
            geometries.iter().try_fold(1_usize, |total, geometry| {
                let geometry_count = wkt_structure_count(geometry)?;
                total
                    .checked_add(geometry_count)
                    .ok_or_else(|| IoError::wkt("WKT structural count overflows usize"))
            })
        },
        _ => Ok(1),
    }
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
#[expect(clippy::too_many_lines, reason = "one match arm per WKT geometry kind")]
fn write_wkt(
    out: &mut String,
    shape: &Shape,
    output_dimension: Option<WktDimension>,
    format: WktNumberFormat,
    limit: usize,
) {
    use std::fmt::Write as _;
    match shape {
        Shape::Point(point) => {
            let axes = wkt_output_axes(CoordinateAxes::from_point(*point), output_dimension);
            let _ = write!(out, "POINT{} (", axes.wkt_tag());
            write_wkt_point(out, *point, axes, format);
            out.push(')');
        },
        Shape::MultiPoint(points) => {
            let _ = write!(
                out,
                "MULTIPOINT{} (",
                wkt_output_axes(shape.axes(), output_dimension).wkt_tag()
            );
            for (index, member) in points.members().iter().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ");
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY"),
                    crate::geometry::MultiMember::Value(point_index) => {
                        let point = points.point_at(*point_index);
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
        },
        Shape::LineString(points) => {
            let axes = wkt_output_axes(shape.axes(), output_dimension);
            let _ = write!(out, "LINESTRING{} (", axes.wkt_tag());
            write_wkt_ring(out, points, axes, format, limit);
            out.push(')');
        },
        Shape::MultiLineString(lines) => {
            let axes = wkt_output_axes(shape.axes(), output_dimension);
            let _ = write!(out, "MULTILINESTRING{} (", axes.wkt_tag());
            for (index, member) in lines.members().iter().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if index > 0 {
                    out.push_str(", ");
                }
                match member {
                    crate::geometry::MultiMember::Empty => out.push_str("EMPTY"),
                    crate::geometry::MultiMember::Value(line_index) => {
                        let line = &lines.as_slice()[*line_index];
                        out.push('(');
                        write_wkt_ring(out, line, axes, format, limit);
                        out.push(')');
                    },
                }
            }
            out.push(')');
        },
        Shape::Polygon(polygon) => {
            let axes = wkt_output_axes(shape.axes(), output_dimension);
            let _ = write!(out, "POLYGON{} (", axes.wkt_tag());
            write_wkt_polygon(out, polygon, axes, format, limit);
            out.push(')');
        },
        Shape::MultiPolygon(polygons) => {
            let axes = wkt_output_axes(shape.axes(), output_dimension);
            let _ = write!(out, "MULTIPOLYGON{} (", axes.wkt_tag());
            for (index, member) in polygons.members().iter().enumerate() {
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
                            &polygons.as_slice()[*polygon_index],
                            axes,
                            format,
                            limit,
                        );
                        out.push(')');
                    },
                }
            }
            out.push(')');
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
                write_wkt(out, geometry, output_dimension, format, limit);
            }
            out.push(')');
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
        },
    }
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
) {
    match (axes.has_z(), axes.has_m()) {
        (false, false) => write_wkt_polygon_impl::<false, false>(out, polygon, format, limit),
        (true, false) => write_wkt_polygon_impl::<true, false>(out, polygon, format, limit),
        (false, true) => write_wkt_polygon_impl::<false, true>(out, polygon, format, limit),
        (true, true) => write_wkt_polygon_impl::<true, true>(out, polygon, format, limit),
    }
}

fn write_wkt_polygon_impl<const Z: bool, const M: bool>(
    out: &mut String,
    polygon: &Polygon,
    format: WktNumberFormat,
    limit: usize,
) {
    out.push('(');
    write_wkt_ring_impl::<Z, M, _>(out, &polygon.shell, format, limit);
    out.push(')');
    for hole in polygon.holes.iter() {
        if out.len() >= limit {
            break;
        }
        out.push_str(", (");
        write_wkt_ring_impl::<Z, M, _>(out, hole, format, limit);
        out.push(')');
    }
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
    // A homogeneous multipart writes every member at the multipart's union
    // axes; a member narrower than the union has its promoted Z/M filled with
    // 0.0 (the `force_3d` convention), NEVER a fabricated NaN. For a scalar or
    // uniform sequence the ordinate is always present, so no fill occurs.
    if Z {
        out.push(' ');
        format.write(out, point.z().unwrap_or(0.0));
    }
    if M {
        out.push(' ');
        format.write(out, point.m().unwrap_or(0.0));
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
