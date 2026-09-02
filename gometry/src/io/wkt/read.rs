#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::{
    CoordSeqBuilder, HasM, HasZ, MultiLineString, MultiMember, MultiPoint, MultiPolygon,
};
use crate::io::wkt::{
    CoordSeq, CoordinateAxes, GeometryKind, IoError, IoGeometryKind, LineSeq, MAX_NESTING,
    ParseFormat, Point, Polygon, Result, Ring, Shape, WktHeader, parse_content,
};

pub(crate) fn parse_wkt(value: &str) -> Result<Shape> {
    parse_wkt_inner(value, 0, None)
        .map_err(|error| parse_content(ParseFormat::Wkt, error))
        // The recursive WKT parser works on borrowed sub-slices and does not
        // carry an original-input cursor. Its public position is therefore the
        // UTF-8 input length for every failure. WKB has a real reader cursor
        // and reports the varying detection offset instead.
        .map_err(|error| error.with_parse_position(value.len()))
}

/// Parse one WKT geometry. `inherited_axes` is set when this geometry is a
/// member of a dimension-tagged GeometryCollection: untagged members adopt
/// those axes; an explicit conflicting tag is rejected. An untagged outer
/// collection leaves this `None` so heterogeneous children remain valid.
#[expect(
    clippy::too_many_lines,
    reason = "WKT kind dispatch is one exhaustive match; splitting obscures coverage"
)]
fn parse_wkt_inner(
    value: &str,
    depth: usize,
    inherited_axes: Option<CoordinateAxes>,
) -> Result<Shape> {
    if depth > MAX_NESTING {
        return Err(IoError::wkt(format!(
            "WKT geometry nesting exceeds the limit of {MAX_NESTING}"
        )));
    }
    let text = value.trim();
    let mut header = parse_wkt_header(text)?;
    let axes = resolve_wkt_member_axes(&header, inherited_axes)?;
    header.axes = axes;
    // Axes are fixed when tagged (compact or spaced) or inherited from a
    // tagged GeometryCollection. Untagged free geometries allow PostGIS EWKT
    // ordinate-count inference (`POINT(1 2 3)` → XYZ, `POINT(1 2 3 4)` → XYZM).
    let axes_fixed = header.axes_explicit || inherited_axes.is_some();
    match header.geometry_type {
        IoGeometryKind::GeometryCollection => {
            let Some(body) = header.body else {
                return Ok(Shape::typed_empty(
                    GeometryKind::GeometryCollection,
                    header.axes,
                ));
            };
            // Propagate an explicit tag on this collection, or an axes constraint
            // inherited from an outer tagged collection, into untagged children.
            let child_axes = if header.axes_explicit {
                Some(header.axes)
            } else {
                inherited_axes
            };
            let members = split_wkt_collection_members(body)?
                .into_iter()
                .map(|value| parse_wkt_inner(value, depth + 1, child_axes))
                .collect::<Result<Vec<_>>>()?;
            Ok(Shape::geometry_collection(members)?)
        },
        IoGeometryKind::MultiPoint => {
            let Some(body) = header.body else {
                return Ok(Shape::typed_empty(GeometryKind::MultiPoint, header.axes));
            };
            let axes = if axes_fixed {
                header.axes
            } else {
                infer_wkt_axes_from_multipoint_body(body)?
            };
            Ok(Shape::MultiPoint(parse_wkt_multi_points(body, axes)?))
        },
        IoGeometryKind::MultiLineString => {
            let Some(body) = header.body else {
                return Ok(Shape::typed_empty(
                    GeometryKind::MultiLineString,
                    header.axes,
                ));
            };
            let members = split_wkt_members(body)?;
            if members.is_empty() {
                return Ok(Shape::typed_empty(
                    GeometryKind::MultiLineString,
                    header.axes,
                ));
            }
            let axes = if axes_fixed {
                header.axes
            } else {
                infer_wkt_axes_from_members(&members)?
            };
            Ok(Shape::MultiLineString(MultiLineString::try_new(
                axes,
                members
                    .into_iter()
                    .map(|member| match member {
                        WktMember::Empty => Ok(MultiMember::Empty),
                        WktMember::Group(line) => Ok(MultiMember::Value(LineSeq::try_new(
                            parse_wkt_points(line, axes)?,
                        )?)),
                    })
                    .collect::<Result<_>>()?,
            )?))
        },
        IoGeometryKind::MultiPolygon => {
            let Some(body) = header.body else {
                return Ok(Shape::typed_empty(GeometryKind::MultiPolygon, header.axes));
            };
            let members = split_wkt_members(body)?;
            let axes = if axes_fixed {
                header.axes
            } else if let Some(first) = members.iter().find_map(|m| match m {
                WktMember::Group(g) => Some(*g),
                WktMember::Empty => None,
            }) {
                infer_wkt_axes_from_polygon_body(first)?
            } else {
                header.axes
            };
            if members.is_empty() {
                return Ok(Shape::typed_empty(GeometryKind::MultiPolygon, axes));
            }
            Ok(Shape::MultiPolygon(MultiPolygon::try_new(
                axes,
                members
                    .into_iter()
                    .map(|member| match member {
                        WktMember::Empty => Ok(MultiMember::Empty),
                        WktMember::Group(polygon) => {
                            Ok(MultiMember::Value(parse_wkt_polygon_body(polygon, axes)?))
                        },
                    })
                    .collect::<Result<_>>()?,
            )?))
        },
        IoGeometryKind::Point => match header.body {
            Some(body) => {
                let axes = if axes_fixed {
                    header.axes
                } else {
                    infer_wkt_axes_from_coordinate(body)?
                };
                Ok(Shape::Point(parse_wkt_point(body, axes)?))
            },
            None => Ok(Shape::typed_empty(GeometryKind::Point, header.axes)),
        },
        IoGeometryKind::LineString => {
            let Some(body) = header.body else {
                return Ok(Shape::typed_empty(GeometryKind::LineString, header.axes));
            };
            let axes = if axes_fixed {
                header.axes
            } else {
                infer_wkt_axes_from_coordinate_list(body)?
            };
            let points = parse_wkt_points(body, axes)?;
            Ok(Shape::LineString(LineSeq::try_new(points)?))
        },
        IoGeometryKind::Polygon => match header.body {
            Some(body) => {
                let axes = if axes_fixed {
                    header.axes
                } else {
                    infer_wkt_axes_from_polygon_body(body)?
                };
                Ok(Shape::Polygon(parse_wkt_polygon_body(body, axes)?))
            },
            None => Ok(Shape::typed_empty(GeometryKind::Polygon, header.axes)),
        },
    }
}

/// PostGIS EWKT ordinate-count inference for an untagged coordinate token list.
fn infer_wkt_axes_from_ordinate_count(count: usize) -> Result<CoordinateAxes> {
    match count {
        2 => Ok(CoordinateAxes::XY),
        3 => Ok(CoordinateAxes::XYZ),
        4 => Ok(CoordinateAxes::XYZM),
        0 | 1 => Err(IoError::wkt("coordinate is missing y")),
        _ => Err(IoError::wkt("coordinate has too many ordinates")),
    }
}

fn infer_wkt_axes_from_coordinate(value: &str) -> Result<CoordinateAxes> {
    infer_wkt_axes_from_ordinate_count(value.split_whitespace().count())
}

fn infer_wkt_axes_from_coordinate_list(value: &str) -> Result<CoordinateAxes> {
    let first = value
        .split(',')
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| IoError::wkt("coordinate is missing x"))?;
    infer_wkt_axes_from_coordinate(first)
}

fn infer_wkt_axes_from_members(members: &[WktMember<'_>]) -> Result<CoordinateAxes> {
    for member in members {
        if let WktMember::Group(body) = member {
            return infer_wkt_axes_from_coordinate_list(body);
        }
    }
    Ok(CoordinateAxes::XY)
}

fn infer_wkt_axes_from_polygon_body(value: &str) -> Result<CoordinateAxes> {
    let members = split_wkt_members(value)?;
    for member in members {
        if let WktMember::Group(ring) = member {
            return infer_wkt_axes_from_coordinate_list(ring);
        }
    }
    Err(IoError::wkt("POLYGON requires a shell"))
}

fn infer_wkt_axes_from_multipoint_body(value: &str) -> Result<CoordinateAxes> {
    if is_wkt_member_list(value) {
        let members = split_wkt_members(value)?;
        for member in members {
            if let WktMember::Group(point) = member {
                return infer_wkt_axes_from_coordinate(point);
            }
        }
        return Ok(CoordinateAxes::XY);
    }
    infer_wkt_axes_from_coordinate_list(value)
}

/// Resolve axes for a geometry that may sit under a tagged GeometryCollection.
fn resolve_wkt_member_axes(
    header: &WktHeader<'_>,
    inherited_axes: Option<CoordinateAxes>,
) -> Result<CoordinateAxes> {
    if header.axes_explicit {
        if let Some(outer) = inherited_axes
            && header.axes != outer
        {
            return Err(IoError::wkt(
                "WKT geometry collection member dimensional tag conflicts with the collection",
            ));
        }
        return Ok(header.axes);
    }
    Ok(inherited_axes.unwrap_or(header.axes))
}

/// Longest type keyword incl. compact PostGIS axes suffix (`GEOMETRYCOLLECTIONZM`).
pub(crate) const WKT_MAX_TYPE_TOKEN_LEN: usize = 20;

/// Strip a trailing compact PostGIS axes suffix (`ZM` / `Z` / `M`).
fn strip_wkt_compact_axes(token: &[u8]) -> (&[u8], Option<CoordinateAxes>) {
    // Longest axes suffix first so `ZM` is not parsed as bare `M`.
    if token.ends_with(b"ZM") {
        return (&token[..token.len() - 2], Some(CoordinateAxes::XYZM));
    }
    if token.ends_with(b"Z") {
        return (&token[..token.len() - 1], Some(CoordinateAxes::XYZ));
    }
    if token.ends_with(b"M") {
        return (&token[..token.len() - 1], Some(CoordinateAxes::XYM));
    }
    (token, None)
}

/// Classify a compact WKT type token (`POINTM`, `MULTILINESTRINGZM`, …).
/// Spaced tags (`POINT M`) are handled by the header parser.
pub(crate) fn classify_wkt_type_token(
    token: &str,
) -> Option<(IoGeometryKind, Option<CoordinateAxes>)> {
    if token.is_empty()
        || token.len() > WKT_MAX_TYPE_TOKEN_LEN
        || !token.as_bytes().iter().all(u8::is_ascii_alphabetic)
    {
        return None;
    }
    let mut upper = [0_u8; WKT_MAX_TYPE_TOKEN_LEN];
    upper[..token.len()].copy_from_slice(token.as_bytes());
    upper[..token.len()].make_ascii_uppercase();
    parse_wkt_type_keyword_upper(&upper[..token.len()])
}

fn parse_wkt_type_keyword_upper(upper: &[u8]) -> Option<(IoGeometryKind, Option<CoordinateAxes>)> {
    let (base, compact_axes) = strip_wkt_compact_axes(upper);
    let geometry_type = match base {
        b"POINT" => IoGeometryKind::Point,
        b"LINESTRING" => IoGeometryKind::LineString,
        b"POLYGON" => IoGeometryKind::Polygon,
        b"MULTIPOINT" => IoGeometryKind::MultiPoint,
        b"MULTILINESTRING" => IoGeometryKind::MultiLineString,
        b"MULTIPOLYGON" => IoGeometryKind::MultiPolygon,
        b"GEOMETRYCOLLECTION" => IoGeometryKind::GeometryCollection,
        _ => return None,
    };
    Some((geometry_type, compact_axes))
}

fn parse_wkt_header(text: &str) -> Result<WktHeader<'_>> {
    let keyword_len = text
        .find(|ch: char| !ch.is_ascii_alphabetic())
        .unwrap_or(text.len());
    // Uppercase the keyword into a stack buffer. Longest token is
    // GEOMETRYCOLLECTIONZM (20). Compact PostGIS suffixes (`POINTM`,
    // `LINESTRINGZ`, …) are accepted alongside spaced tags (`POINT M`).
    let keyword = text
        .get(..keyword_len)
        .ok_or_else(|| IoError::wkt("invalid WKT geometry type"))?;
    if keyword_len == 0 || keyword_len > WKT_MAX_TYPE_TOKEN_LEN {
        return Err(IoError::wkt(unsupported_wkt_geometry_type_message(keyword)));
    }
    let Some((geometry_type, compact_axes)) = classify_wkt_type_token(keyword) else {
        return Err(IoError::wkt(unsupported_wkt_geometry_type_message(keyword)));
    };
    let mut rest = text
        .get(keyword_len..)
        .ok_or_else(|| IoError::wkt("invalid WKT geometry type"))?
        .trim_start();
    let (mut axes, mut axes_explicit) =
        compact_axes.map_or((CoordinateAxes::XY, false), |compact| (compact, true));
    if let Some(axis_len) = rest
        .find(|ch: char| !ch.is_ascii_alphabetic())
        .filter(|_| !rest.starts_with("EMPTY"))
    {
        if axis_len > 0 {
            // Spaced axis tags (`POINT Z`, `LINESTRING M`) — rejected when a
            // compact suffix already fixed the axes (e.g. `POINTM Z`).
            if axes_explicit {
                return Err(IoError::wkt("invalid WKT dimensional tag"));
            }
            let axis = rest
                .get(..axis_len)
                .ok_or_else(|| IoError::wkt("invalid WKT dimensional tag"))?;
            axes = if axis.eq_ignore_ascii_case("Z") {
                CoordinateAxes::XYZ
            } else if axis.eq_ignore_ascii_case("M") {
                CoordinateAxes::XYM
            } else if axis.eq_ignore_ascii_case("ZM") {
                CoordinateAxes::XYZM
            } else {
                return Err(IoError::wkt("invalid WKT dimensional tag"));
            };
            axes_explicit = true;
            rest = rest
                .get(axis_len..)
                .ok_or_else(|| IoError::wkt("invalid WKT dimensional tag"))?
                .trim_start();
        }
    } else if !rest.is_empty() && !rest.starts_with('(') && !rest.eq_ignore_ascii_case("EMPTY") {
        return Err(IoError::wkt("invalid WKT dimensional tag"));
    }

    if rest.eq_ignore_ascii_case("EMPTY") {
        return Ok(WktHeader {
            geometry_type,
            axes,
            axes_explicit,
            body: None,
        });
    }
    if !rest.starts_with('(') {
        return Err(IoError::wkt("WKT is missing opening parenthesis"));
    }
    Ok(WktHeader {
        geometry_type,
        axes,
        axes_explicit,
        body: Some(paren_body(rest)?),
    })
}

fn unsupported_wkt_geometry_type_message(keyword: &str) -> String {
    let rendered = match keyword {
        "CIRCULARSTRING" => "CircularString",
        "COMPOUNDCURVE" => "CompoundCurve",
        "CURVEPOLYGON" | "CURVEDPOLYGON" => "CurvePolygon",
        "MULTICURVE" => "MultiCurve",
        "MULTISURFACE" => "MultiSurface",
        "TRIANGLE" => "Triangle",
        "TIN" => "TIN",
        "POLYHEDRALSURFACE" => "PolyhedralSurface",
        "" => return "unsupported WKT geometry type".to_owned(),
        value => value,
    };
    format!("unsupported WKT geometry type {rendered}")
}

fn paren_body(value: &str) -> Result<&str> {
    let value = value.trim();
    if !value.starts_with('(') {
        return Err(IoError::wkt("WKT is missing opening parenthesis"));
    }
    let mut depth = 0_u32;
    for (idx, ch) in value.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| IoError::wkt("invalid WKT parentheses"))?;
                if depth == 0 {
                    if !value
                        .get(idx + 1..)
                        .ok_or_else(|| IoError::wkt("invalid WKT parentheses"))?
                        .trim()
                        .is_empty()
                    {
                        return Err(IoError::wkt("trailing text after WKT geometry"));
                    }
                    return value
                        .get(1..idx)
                        .map(str::trim)
                        .ok_or_else(|| IoError::wkt("invalid WKT parentheses"));
                }
            },
            _ => {},
        }
    }
    Err(IoError::wkt("WKT is missing closing parenthesis"))
}

fn parse_wkt_polygon_body(value: &str, axes: CoordinateAxes) -> Result<Polygon> {
    let members = split_wkt_members(value)?;
    let mut rings = members.into_iter().map(|member| match member {
        WktMember::Empty => Err(IoError::wkt("POLYGON ring cannot be EMPTY")),
        WktMember::Group(ring) => parse_wkt_ring(ring, axes),
    });
    let Some(shell) = rings.next() else {
        return Err(IoError::wkt("POLYGON requires a shell"));
    };
    Ok(Polygon::new(shell?, rings.collect::<Result<_>>()?))
}

/// Strict GeometryCollection member list: comma-separated full WKT geometries.
/// Consumes every byte; rejects trailing/leading commas, empty slots, and
/// non-keyword garbage between members.
fn split_wkt_collection_members(value: &str) -> Result<Vec<&str>> {
    let bytes = value.as_bytes();
    let mut index = 0_usize;
    let mut members = Vec::new();

    skip_wkt_ascii_whitespace(bytes, &mut index);
    // Empty parenthesized bodies (`GEOMETRYCOLLECTION ()`) are invalid; the
    // empty spelling is the `EMPTY` keyword, not `()`.
    if index >= bytes.len() {
        return Err(IoError::wkt(
            "empty parenthesized WKT member list; use EMPTY for an empty geometry",
        ));
    }

    loop {
        skip_wkt_ascii_whitespace(bytes, &mut index);
        if index >= bytes.len() {
            return Err(IoError::wkt("trailing comma in geometry collection"));
        }
        // Members are full WKT geometries — must begin with a type keyword.
        if !bytes[index].is_ascii_alphabetic() {
            return Err(IoError::wkt(
                "expected geometry type in geometry collection member list",
            ));
        }
        let start = index;
        let mut depth = 0_u32;
        while index < bytes.len() {
            match bytes[index] {
                b'(' => depth += 1,
                b')' => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or_else(|| IoError::wkt("invalid geometry collection parentheses"))?;
                },
                b',' if depth == 0 => break,
                _ => {},
            }
            index += 1;
        }
        if depth != 0 {
            return Err(IoError::wkt("unclosed geometry collection parentheses"));
        }
        let member = value
            .get(start..index)
            .ok_or_else(|| IoError::wkt("invalid geometry collection member"))?
            .trim_end();
        if member.is_empty() {
            return Err(IoError::wkt("empty geometry collection member"));
        }
        members.push(member);
        if index >= bytes.len() {
            return Ok(members);
        }
        // Comma at depth 0 — another member is required.
        debug_assert_eq!(bytes[index], b',');
        index += 1;
    }
}

fn parse_wkt_ring(value: &str, axes: CoordinateAxes) -> Result<Ring> {
    // Shared untrusted admission: silent-close XY-open; reject Z/M-open (see
    // [`crate::io::admit_closed_ring`]).
    crate::io::admit_closed_ring(parse_wkt_points(value, axes)?)
}

fn parse_wkt_multi_points(value: &str, axes: CoordinateAxes) -> Result<MultiPoint> {
    // Parenthesized / EMPTY members vs bare "x y, x y" coordinate list.
    if is_wkt_member_list(value) {
        let members = split_wkt_members(value)?;
        return MultiPoint::try_new(
            axes,
            members
                .into_iter()
                .map(|member| match member {
                    WktMember::Empty => Ok(MultiMember::Empty),
                    WktMember::Group(point) => {
                        Ok(MultiMember::Value(parse_wkt_point(point, axes)?))
                    },
                })
                .collect::<Result<_>>()?,
        );
    }
    match (axes.has_z(), axes.has_m()) {
        (false, false) => parse_wkt_points_impl::<false, false>(value).map(MultiPoint::from),
        (true, false) => parse_wkt_points_impl::<true, false>(value).map(MultiPoint::from),
        (false, true) => parse_wkt_points_impl::<false, true>(value).map(MultiPoint::from),
        (true, true) => parse_wkt_points_impl::<true, true>(value).map(MultiPoint::from),
    }
}

/// True when the multipoint body uses parenthesized members and/or EMPTY
/// keywords rather than a bare coordinate list (`1 2, 3 4`).
fn is_wkt_member_list(value: &str) -> bool {
    let value = value.trim_start();
    value.starts_with('(')
        || (value.len() >= 5
            && value
                .as_bytes()
                .get(..5)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"EMPTY"))
            && value
                .as_bytes()
                .get(5)
                .is_none_or(|byte| !byte.is_ascii_alphanumeric()))
}

/// One member of a multipart WKT body: the EMPTY keyword or a parenthesized group.
#[derive(Debug, Clone, Copy)]
enum WktMember<'a> {
    Empty,
    Group(&'a str),
}

/// Strict group-list scanner for multipart members and polygon rings.
///
/// Yields EMPTY keywords and parenthesized groups. Consumes every
/// non-whitespace byte, requires commas between members, and rejects trailing
/// commas and leftover garbage. GeometryCollection uses
/// [`split_wkt_collection_members`] (full child WKT texts, not groups).
fn split_wkt_members(value: &str) -> Result<Vec<WktMember<'_>>> {
    let bytes = value.as_bytes();
    let mut index = 0_usize;
    let mut members = Vec::new();

    skip_wkt_ascii_whitespace(bytes, &mut index);
    // Empty parenthesized bodies (`MULTILINESTRING ()`) are invalid; the
    // empty spelling is the `EMPTY` keyword, not `()`.
    if index >= bytes.len() {
        return Err(IoError::wkt(
            "empty parenthesized WKT member list; use EMPTY for an empty geometry",
        ));
    }

    loop {
        skip_wkt_ascii_whitespace(bytes, &mut index);
        if index >= bytes.len() {
            return Err(IoError::wkt("trailing comma in WKT member list"));
        }

        if is_wkt_empty_keyword(bytes, index) {
            index += 5;
            members.push(WktMember::Empty);
        } else if bytes[index] == b'(' {
            index += 1;
            let start = index;
            let mut depth = 1_u32;
            let mut closed = false;
            while index < bytes.len() {
                match bytes[index] {
                    b'(' => depth += 1,
                    b')' => {
                        depth = depth
                            .checked_sub(1)
                            .ok_or_else(|| IoError::wkt("invalid WKT member parentheses"))?;
                        if depth == 0 {
                            let group = value
                                .get(start..index)
                                .ok_or_else(|| IoError::wkt("invalid WKT member parentheses"))?
                                .trim();
                            index += 1;
                            members.push(WktMember::Group(group));
                            closed = true;
                            break;
                        }
                    },
                    _ => {},
                }
                index += 1;
            }
            if !closed {
                return Err(IoError::wkt("unclosed WKT member parentheses"));
            }
        } else {
            return Err(IoError::wkt(
                "expected WKT member (parenthesized group or EMPTY)",
            ));
        }

        skip_wkt_ascii_whitespace(bytes, &mut index);
        if index >= bytes.len() {
            return Ok(members);
        }
        if bytes[index] == b',' {
            index += 1;
            continue;
        }
        return Err(IoError::wkt("expected comma between WKT members"));
    }
}

const fn skip_wkt_ascii_whitespace(bytes: &[u8], index: &mut usize) {
    while *index < bytes.len() && bytes[*index].is_ascii_whitespace() {
        *index += 1;
    }
}

fn is_wkt_empty_keyword(bytes: &[u8], index: usize) -> bool {
    bytes.len() >= index + 5
        && bytes[index..index + 5].eq_ignore_ascii_case(b"EMPTY")
        && (bytes.len() == index + 5 || !bytes[index + 5].is_ascii_alphanumeric())
}

fn parse_wkt_points(value: &str, axes: CoordinateAxes) -> Result<CoordSeq> {
    match (axes.has_z(), axes.has_m()) {
        (false, false) => parse_wkt_points_impl::<false, false>(value),
        (true, false) => parse_wkt_points_impl::<true, false>(value),
        (false, true) => parse_wkt_points_impl::<false, true>(value),
        (true, true) => parse_wkt_points_impl::<true, true>(value),
    }
}

fn parse_wkt_points_impl<const Z: bool, const M: bool>(value: &str) -> Result<CoordSeq> {
    let bytes = value.as_bytes();
    let mut capacity = 1;
    for &byte in bytes {
        capacity += usize::from(byte == b',');
    }
    match parse_wkt_ascii_points_impl::<Z, M>(bytes, capacity) {
        Ok(sequence) => Ok(sequence),
        Err(error) if bytes.is_ascii() => Err(error),
        Err(_) => {
            let mut builder =
                CoordSeqBuilder::with_capacity(CoordinateAxes::new(HasZ(Z), HasM(M)), capacity);
            for coordinate in value.split(',') {
                parse_wkt_coordinate_into::<Z, M>(coordinate, &mut builder)?;
            }
            builder.finish()
        },
    }
}

/// Parse the ASCII WKT coordinate lists produced by normal serializers without
/// layering `split(',')` and `split_whitespace()` iterators for every vertex.
/// Non-ASCII input falls back to the Unicode-aware path above if byte parsing
/// encounters it.
fn parse_wkt_ascii_points_impl<const Z: bool, const M: bool>(
    bytes: &[u8],
    capacity: usize,
) -> Result<CoordSeq> {
    let mut builder =
        CoordSeqBuilder::with_capacity(CoordinateAxes::new(HasZ(Z), HasM(M)), capacity);
    let mut index = 0;
    loop {
        let x = parse_wkt_ascii_float_at(bytes, &mut index, "x", true)?;
        let y = parse_wkt_ascii_float_at(bytes, &mut index, "y", true)?;
        let z = Z
            .then(|| parse_wkt_ascii_float_at(bytes, &mut index, "z", true))
            .transpose()?;
        let m = M
            .then(|| parse_wkt_ascii_float_at(bytes, &mut index, "m", true))
            .transpose()?;
        skip_wkt_ascii_whitespace(bytes, &mut index);
        if index < bytes.len() && bytes[index] != b',' {
            return Err(IoError::wkt("coordinate has too many ordinates"));
        }
        builder.push_xyzm(x, y, z, m);
        if index == bytes.len() {
            break;
        }
        index += 1;
    }
    builder.finish()
}

fn parse_wkt_coordinate_into<const Z: bool, const M: bool>(
    value: &str,
    builder: &mut CoordSeqBuilder,
) -> Result<()> {
    let mut values = value.split_whitespace();
    let x = parse_wkt_float_token(
        values
            .next()
            .ok_or_else(|| IoError::wkt("coordinate is missing x"))?,
        "x",
    )?;
    let y = parse_wkt_float_token(
        values
            .next()
            .ok_or_else(|| IoError::wkt("coordinate is missing y"))?,
        "y",
    )?;
    let z = Z
        .then(|| parse_wkt_ordinate(&mut values, "z"))
        .transpose()?;
    let m = M
        .then(|| parse_wkt_ordinate(&mut values, "m"))
        .transpose()?;
    if values.next().is_some() {
        return Err(IoError::wkt("coordinate has too many ordinates"));
    }
    builder.push_xyzm(x, y, z, m);
    Ok(())
}

fn parse_wkt_float_token(token: &str, name: &str) -> Result<f64> {
    lexical_core::parse::<f64>(token.as_bytes())
        .map_err(|_| IoError::wkt(format!("{name} coordinate must be numeric")))
}

fn parse_wkt_point(value: &str, axes: CoordinateAxes) -> Result<Point> {
    match (axes.has_z(), axes.has_m()) {
        (false, false) => parse_wkt_point_impl::<false, false>(value),
        (true, false) => parse_wkt_point_impl::<true, false>(value),
        (false, true) => parse_wkt_point_impl::<false, true>(value),
        (true, true) => parse_wkt_point_impl::<true, true>(value),
    }
}

fn parse_wkt_point_impl<const Z: bool, const M: bool>(value: &str) -> Result<Point> {
    let bytes = value.as_bytes();
    match parse_wkt_ascii_point_impl::<Z, M>(bytes) {
        Ok(point) => return Ok(point),
        Err(error) if bytes.is_ascii() => return Err(error),
        Err(_) => {},
    }
    let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::new(HasZ(Z), HasM(M)), 1);
    parse_wkt_coordinate_into::<Z, M>(value, &mut builder)?;
    finish_wkt_point(builder)
}

fn parse_wkt_ascii_point_impl<const Z: bool, const M: bool>(bytes: &[u8]) -> Result<Point> {
    let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::new(HasZ(Z), HasM(M)), 1);
    let mut index = 0;
    let x = parse_wkt_ascii_float_at(bytes, &mut index, "x", false)?;
    let y = parse_wkt_ascii_float_at(bytes, &mut index, "y", false)?;
    let z = Z
        .then(|| parse_wkt_ascii_float_at(bytes, &mut index, "z", false))
        .transpose()?;
    let m = M
        .then(|| parse_wkt_ascii_float_at(bytes, &mut index, "m", false))
        .transpose()?;
    skip_wkt_ascii_whitespace(bytes, &mut index);
    if index != bytes.len() {
        return Err(IoError::wkt("coordinate has too many ordinates"));
    }
    builder.push_xyzm(x, y, z, m);
    finish_wkt_point(builder)
}

fn finish_wkt_point(builder: CoordSeqBuilder) -> Result<Point> {
    builder
        .finish()?
        .first()
        .ok_or_else(|| IoError::wkt("coordinate is missing x"))
}

/// Read one ASCII numeric token. The comma delimiter is enabled for coordinate
/// lists and disabled for Points so malformed point commas retain the old
/// numeric-token error.
#[inline(always)]
fn parse_wkt_ascii_float_at(
    bytes: &[u8],
    index: &mut usize,
    name: &str,
    comma_delimiter: bool,
) -> Result<f64> {
    skip_wkt_ascii_whitespace(bytes, index);
    let start = *index;
    while *index < bytes.len()
        && !bytes[*index].is_ascii_whitespace()
        && (!comma_delimiter || bytes[*index] != b',')
    {
        *index += 1;
    }
    if start == *index {
        return Err(IoError::wkt(format!("coordinate is missing {name}")));
    }
    lexical_core::parse::<f64>(&bytes[start..*index])
        .map_err(|_| IoError::wkt(format!("{name} coordinate must be numeric")))
}

fn parse_wkt_ordinate<'a>(values: &mut impl Iterator<Item = &'a str>, name: &str) -> Result<f64> {
    parse_wkt_float_token(
        values
            .next()
            .ok_or_else(|| IoError::wkt(format!("coordinate is missing {name}")))?,
        name,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_coordinate_cursor_preserves_float_bits() {
        let sequence = parse_wkt_ascii_points_impl::<false, false>(
            b"-0.0 1.2345678901234567, 1e308 -1e-308",
            2,
        )
        .expect("ASCII coordinate list parses");
        assert_eq!(sequence.xs()[0].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(sequence.ys()[0].to_bits(), 1.2345678901234567_f64.to_bits());
        assert_eq!(sequence.xs()[1].to_bits(), 1e308_f64.to_bits());
        assert_eq!(sequence.ys()[1].to_bits(), (-1e-308_f64).to_bits());
    }

    #[test]
    fn ascii_point_cursor_preserves_ordinates() {
        let point =
            parse_wkt_ascii_point_impl::<true, true>(b"1 2 3 4").expect("ASCII point parses");
        assert_eq!(point.x, 1.0);
        assert_eq!(point.y, 2.0);
        assert_eq!(point.z(), Some(3.0));
        assert_eq!(point.m(), Some(4.0));
    }

    #[test]
    fn coordinate_dispatch_preserves_unicode_whitespace_compatibility() {
        let point = parse_wkt("POINT ZM (1\u{00a0}2\u{2003}3\u{2028}4)")
            .expect("Unicode whitespace remains accepted");
        let Shape::Point(point) = point else {
            panic!("expected point");
        };
        assert_eq!(
            (point.x, point.y, point.z(), point.m()),
            (1.0, 2.0, Some(3.0), Some(4.0))
        );

        let line = parse_wkt("LINESTRING (1\u{2003}2, 3\u{2028}4)")
            .expect("Unicode whitespace remains accepted in lists");
        let Shape::LineString(line) = line else {
            panic!("expected line string");
        };
        assert_eq!(line.as_coords().xs(), &[1.0, 3.0]);
        assert_eq!(line.as_coords().ys(), &[2.0, 4.0]);
    }

    #[test]
    fn coordinate_dispatch_rejects_non_whitespace_unicode_in_numbers() {
        let error = parse_wkt("POINT (1\u{00e9} 2)").expect_err("Unicode character is invalid");
        assert!(error.to_string().contains("x coordinate must be numeric"));

        let error = parse_wkt("POINT (1\u{00a0}not-a-number)")
            .expect_err("Unicode whitespace cannot make a bad number valid");
        assert!(error.to_string().contains("y coordinate must be numeric"));
    }
}
