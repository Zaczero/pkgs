use super::{CoordinateAxes, GeometryKind, IoError, ParseFormat, Result, WktHeader, WktParser};

impl WktParser<'_> {
    pub(super) fn infer_axes(
        &self,
        comma_delimiter: bool,
        empty_means_missing_x: bool,
    ) -> Result<CoordinateAxes> {
        let start = self.position();
        let end = self.coordinate_group_end(start, comma_delimiter)?;
        let coordinate = self
            .input
            .get(start..end)
            .ok_or_else(|| self.error_at(start, "invalid WKT coordinate"))?;
        if empty_means_missing_x && coordinate.trim().is_empty() {
            return self.fail_at(end, "coordinate is missing x");
        }
        let count = coordinate.split_whitespace().count();
        infer_wkt_axes_from_ordinate_count(count).map_err(|error| error.with_parse_position(end))
    }

    pub(super) fn coordinate_group_end(
        &self,
        start: usize,
        comma_delimiter: bool,
    ) -> Result<usize> {
        let bytes = self.cursor.remaining;
        let mut nested = 0_usize;
        for (offset, byte) in bytes.iter().copied().enumerate() {
            if comma_delimiter && byte == b',' {
                return start
                    .checked_add(offset)
                    .ok_or_else(|| self.error_at(start, "WKT input position overflow"));
            }
            if byte == b'(' {
                nested = nested
                    .checked_add(1)
                    .ok_or_else(|| self.error_at(start, "WKT nesting depth overflow"))?;
            } else if byte == b')' {
                if nested == 0 {
                    return start
                        .checked_add(offset)
                        .ok_or_else(|| self.error_at(start, "WKT input position overflow"));
                }
                nested = nested
                    .checked_sub(1)
                    .ok_or_else(|| self.error_at(start, "WKT nesting depth underflow"))?;
            }
        }
        Ok(self.input.len())
    }
}

/// Longest type keyword incl. compact PostGIS axes suffix (`GEOMETRYCOLLECTIONZM`).
const WKT_MAX_TYPE_TOKEN_LEN: usize = 20;

fn infer_wkt_axes_from_ordinate_count(count: usize) -> Result<CoordinateAxes> {
    match count {
        2 => Ok(CoordinateAxes::XY),
        3 => Ok(CoordinateAxes::XYZ),
        4 => Ok(CoordinateAxes::XYZM),
        0 | 1 => Err(IoError::wkt("coordinate is missing y")),
        _ => Err(IoError::wkt("coordinate has too many ordinates")),
    }
}

/// Strip a trailing compact PostGIS axes suffix (`ZM` / `Z` / `M`).
pub(super) fn strip_wkt_compact_axes(token: &[u8]) -> (&[u8], Option<CoordinateAxes>) {
    if let Some(base) = token.strip_suffix(b"ZM") {
        return (base, Some(CoordinateAxes::XYZM));
    }
    if let Some(base) = token.strip_suffix(b"Z") {
        return (base, Some(CoordinateAxes::XYZ));
    }
    if let Some(base) = token.strip_suffix(b"M") {
        return (base, Some(CoordinateAxes::XYM));
    }
    (token, None)
}

/// Classify a compact WKT type token (`POINTM`, `MULTILINESTRINGZM`, …).
/// Spaced tags (`POINT M`) are handled by the header parser.
pub(crate) fn classify_wkt_type_token(
    token: &str,
) -> Option<(GeometryKind, Option<CoordinateAxes>)> {
    if token.is_empty()
        || token.len() > WKT_MAX_TYPE_TOKEN_LEN
        || !token.as_bytes().iter().all(u8::is_ascii_alphabetic)
    {
        return None;
    }
    let mut upper = [0_u8; WKT_MAX_TYPE_TOKEN_LEN];
    let upper_keyword = upper.get_mut(..token.len())?;
    upper_keyword.copy_from_slice(token.as_bytes());
    upper_keyword.make_ascii_uppercase();
    parse_wkt_type_keyword_upper(upper.get(..token.len())?)
}

pub(super) fn parse_wkt_type_keyword_upper(
    upper: &[u8],
) -> Option<(GeometryKind, Option<CoordinateAxes>)> {
    let (base, compact_axes) = strip_wkt_compact_axes(upper);
    let geometry_type = match base {
        b"POINT" => GeometryKind::Point,
        b"LINESTRING" => GeometryKind::LineString,
        b"POLYGON" => GeometryKind::Polygon,
        b"MULTIPOINT" => GeometryKind::MultiPoint,
        b"MULTILINESTRING" => GeometryKind::MultiLineString,
        b"MULTIPOLYGON" => GeometryKind::MultiPolygon,
        b"GEOMETRYCOLLECTION" => GeometryKind::GeometryCollection,
        _ => return None,
    };
    Some((geometry_type, compact_axes))
}

pub(super) fn parse_wkt_header(
    source: &str,
    body_opened: bool,
    input_offset: usize,
) -> Result<WktHeader> {
    let header_error =
        |position: usize, message: Box<str>| match checked_wkt_position(input_offset, position) {
            Ok(position) => IoError::parse_at(ParseFormat::Wkt, position, message),
            Err(error) => error,
        };
    let leading_whitespace = source
        .len()
        .checked_sub(source.trim_start().len())
        .ok_or_else(|| header_error(0, "WKT input position overflow".into()))?;
    let text = source.trim();
    let keyword_len = text
        .find(|ch: char| !ch.is_ascii_alphabetic())
        .unwrap_or(text.len());
    let keyword = text
        .get(..keyword_len)
        .ok_or_else(|| header_error(leading_whitespace, "invalid WKT geometry type".into()))?;
    if keyword_len == 0 || keyword_len > WKT_MAX_TYPE_TOKEN_LEN {
        return Err(header_error(
            leading_whitespace,
            unsupported_wkt_geometry_type_message(keyword).into_boxed_str(),
        ));
    }
    let Some((geometry_type, compact_axes)) = classify_wkt_type_token(keyword) else {
        return Err(header_error(
            leading_whitespace,
            unsupported_wkt_geometry_type_message(keyword).into_boxed_str(),
        ));
    };
    let raw_rest = text
        .get(keyword_len..)
        .ok_or_else(|| header_error(leading_whitespace, "invalid WKT geometry type".into()))?;
    let mut rest = raw_rest.trim_start();
    let leading_rest_whitespace = raw_rest
        .len()
        .checked_sub(rest.len())
        .ok_or_else(|| header_error(leading_whitespace, "invalid WKT geometry header".into()))?;
    let mut rest_position = leading_whitespace
        .checked_add(keyword_len)
        .and_then(|position| position.checked_add(leading_rest_whitespace))
        .ok_or_else(|| header_error(leading_whitespace, "WKT input position overflow".into()))?;
    let (mut axes, mut axes_explicit) =
        compact_axes.map_or((CoordinateAxes::XY, false), |compact| (compact, true));
    let compact_axes_length = match compact_axes {
        Some(CoordinateAxes::XYZM) => 2,
        Some(_) => 1,
        None => 0,
    };
    let mut axes_position = if compact_axes.is_some() {
        let suffix_start = keyword_len
            .checked_sub(compact_axes_length)
            .ok_or_else(|| {
                header_error(
                    leading_whitespace,
                    "invalid compact WKT dimensional tag".into(),
                )
            })?;
        let position = leading_whitespace
            .checked_add(suffix_start)
            .ok_or_else(|| {
                header_error(leading_whitespace, "WKT input position overflow".into())
            })?;
        Some(checked_wkt_position(input_offset, position)?)
    } else {
        None
    };
    if let Some(axis_len) = rest
        .find(|ch: char| !ch.is_ascii_alphabetic())
        .filter(|_| !rest.starts_with("EMPTY"))
    {
        if axis_len > 0 {
            if axes_explicit {
                return Err(header_error(
                    rest_position,
                    "invalid WKT dimensional tag".into(),
                ));
            }
            let axis = rest
                .get(..axis_len)
                .ok_or_else(|| header_error(rest_position, "invalid WKT dimensional tag".into()))?;
            axes = if axis.eq_ignore_ascii_case("Z") {
                CoordinateAxes::XYZ
            } else if axis.eq_ignore_ascii_case("M") {
                CoordinateAxes::XYM
            } else if axis.eq_ignore_ascii_case("ZM") {
                CoordinateAxes::XYZM
            } else {
                return Err(header_error(
                    rest_position,
                    "invalid WKT dimensional tag".into(),
                ));
            };
            axes_explicit = true;
            axes_position = Some(checked_wkt_position(input_offset, rest_position)?);
            let after_axis = rest
                .get(axis_len..)
                .ok_or_else(|| header_error(rest_position, "invalid WKT dimensional tag".into()))?;
            rest = after_axis.trim_start();
            let following_whitespace = after_axis
                .len()
                .checked_sub(rest.len())
                .ok_or_else(|| header_error(rest_position, "invalid WKT dimensional tag".into()))?;
            rest_position = rest_position
                .checked_add(axis_len)
                .and_then(|position| position.checked_add(following_whitespace))
                .ok_or_else(|| header_error(rest_position, "WKT input position overflow".into()))?;
        }
    } else if !rest.is_empty() && !rest.starts_with('(') && !rest.eq_ignore_ascii_case("EMPTY") {
        return Err(header_error(
            rest_position,
            "invalid WKT dimensional tag".into(),
        ));
    }

    if rest.eq_ignore_ascii_case("EMPTY") {
        return Ok(WktHeader {
            geometry_type,
            axes,
            axes_explicit,
            axes_position,
            body: None,
        });
    }
    if !body_opened || !rest.starts_with('(') {
        let position = if rest.is_empty() {
            source.len()
        } else {
            rest_position
        };
        return Err(header_error(
            position,
            "WKT is missing opening parenthesis".into(),
        ));
    }
    Ok(WktHeader {
        geometry_type,
        axes,
        axes_explicit,
        axes_position,
        body: Some(()),
    })
}

pub(super) fn checked_wkt_position(offset: usize, relative: usize) -> Result<usize> {
    offset
        .checked_add(relative)
        .ok_or_else(|| IoError::parse_at(ParseFormat::Wkt, offset, "WKT input position overflow"))
}

pub(super) fn unsupported_wkt_geometry_type_message(keyword: &str) -> String {
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
