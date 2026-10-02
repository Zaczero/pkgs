#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
#[cfg(test)]
use std::cell::Cell;

use crate::geometry::{
    CoordSeqBuilder, HasM, HasZ, MOrdinate, MultiLineString, MultiMember, MultiPoint, MultiPolygon,
    ZOrdinate,
};
use crate::io::{
    CoordSeq, CoordinateAxes, GeometryKind, IoError, LineSeq, MAX_NESTING, ParseFormat, Point,
    Polygon, Result, Shape, parse_content,
};

#[cfg(test)]
std::thread_local! {
    static WKT_CURSOR_WORK: Cell<usize> = const { Cell::new(0) };
}

macro_rules! skip_wkt_whitespace {
    ($parser:ident) => {{
        loop {
            let Some(byte) = $parser.cursor.peek() else {
                break;
            };
            if byte.is_ascii_whitespace() {
                $parser.cursor.advance();
                continue;
            }
            if byte.is_ascii() {
                break;
            }
            let Some(character) = $parser
                .input
                .get($parser.cursor.position..)
                .and_then(|tail| tail.chars().next())
            else {
                break;
            };
            if !character.is_whitespace() {
                break;
            }
            $parser.cursor.advance_by(character.len_utf8());
        }
    }};
}

macro_rules! parse_wkt_ordinate {
    ($parser:ident, $name:literal, $comma_delimiter:expr) => {{
        skip_wkt_whitespace!($parser);
        let start = $parser.cursor.position;
        while let Some(byte) = $parser.cursor.peek() {
            if byte == b')' || ($comma_delimiter && byte == b',') || byte.is_ascii_whitespace() {
                break;
            }
            if byte == b'(' {
                $parser.open_parenthesis();
            } else if byte.is_ascii() {
                $parser.cursor.advance();
            } else {
                let character = $parser
                    .input
                    .get($parser.cursor.position..)
                    .and_then(|tail| tail.chars().next())
                    .ok_or_else(|| $parser.error_at(start, "invalid WKT coordinate"))?;
                if character.is_whitespace() {
                    break;
                }
                $parser.cursor.advance_by(character.len_utf8());
            }
        }
        if start == $parser.cursor.position {
            return $parser.fail(format!("coordinate is missing {}", $name));
        }
        let token = $parser
            .cursor
            .bytes
            .get(start..$parser.cursor.position)
            .ok_or_else(|| $parser.error_at(start, "invalid WKT coordinate"))?;
        lexical_core::parse::<f64>(token)
            .map_err(|_| $parser.error_at(start, format!("{} coordinate must be numeric", $name)))?
    }};
}

macro_rules! read_wkt_coordinate_axes {
    ($parser:ident, $has_z:ident, $has_m:ident, $comma_delimiter:expr) => {{
        let x = parse_wkt_ordinate!($parser, "x", $comma_delimiter);
        let y = parse_wkt_ordinate!($parser, "y", $comma_delimiter);
        let z = if $has_z {
            Some(parse_wkt_ordinate!($parser, "z", $comma_delimiter))
        } else {
            None
        };
        let m = if $has_m {
            Some(parse_wkt_ordinate!($parser, "m", $comma_delimiter))
        } else {
            None
        };
        (x, y, z, m)
    }};
}

pub(crate) fn parse_wkt(value: &str) -> Result<Shape> {
    let mut parser = WktParser::new(value);
    parser
        .parse_document()
        .map_err(|error| parse_content(ParseFormat::Wkt, error))
        .map_err(|error| error.with_parse_position(parser.cursor.position))
}

struct WktParser<'a> {
    input: &'a str,
    cursor: WktByteCursor<'a>,
    parenthesis_depth: usize,
}

struct WktHeader {
    geometry_type: GeometryKind,
    axes: CoordinateAxes,
    axes_explicit: bool,
    axes_position: Option<usize>,
    body: Option<()>,
}

#[derive(Clone, Copy)]
enum ParentScope {
    Root,
    Collection,
}

impl<'a> WktParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            cursor: WktByteCursor::new(input.as_bytes()),
            parenthesis_depth: 0,
        }
    }

    fn parse_document(&mut self) -> Result<Shape> {
        self.parse_geometry(0, None, ParentScope::Root)
            .map_err(|error| self.position_error(error))
    }

    fn parse_geometry(
        &mut self,
        depth: usize,
        inherited_axes: Option<CoordinateAxes>,
        parent_scope: ParentScope,
    ) -> Result<Shape> {
        if depth > MAX_NESTING {
            return self.fail(format!(
                "WKT geometry nesting exceeds the limit of {MAX_NESTING}"
            ));
        }

        let header = self
            .read_header(parent_scope)
            .map_err(|error| self.position_error(error))?;
        let axes = match resolve_wkt_member_axes(&header, inherited_axes) {
            Ok(axes) => axes,
            Err(error) => {
                let position = header.axes_position.unwrap_or(self.cursor.position);
                return Err(error.with_parse_position(position));
            },
        };
        let axes_fixed = header.axes_explicit || inherited_axes.is_some();
        if header.body.is_none() {
            self.check_parent_boundary(parent_scope)?;
            return Ok(Shape::typed_empty(header.geometry_type, axes));
        }

        let body_depth = self.parenthesis_depth;
        let result = self.parse_body(header.geometry_type, axes, axes_fixed, depth, &header);
        let shape = match result {
            Ok(shape) => shape,
            Err(error) => {
                let error = self.position_error(error);
                if !self.recover_to_geometry_close(body_depth) {
                    return self.fail("WKT is missing closing parenthesis");
                }
                self.check_parent_boundary(parent_scope)?;
                return Err(error);
            },
        };

        self.close_parenthesis()?;
        self.check_parent_boundary(parent_scope)?;
        Ok(shape)
    }

    fn read_header(&mut self, parent_scope: ParentScope) -> Result<WktHeader> {
        let start = self.cursor.position;
        let mut body_opened = false;
        while let Some(byte) = self.cursor.peek() {
            if byte == b'(' {
                self.open_parenthesis();
                body_opened = true;
                break;
            }
            if matches!(parent_scope, ParentScope::Collection) && matches!(byte, b',' | b')') {
                break;
            }
            self.advance_char();
        }

        let text = self
            .input
            .get(start..self.cursor.position)
            .ok_or_else(|| self.error_at(start, "invalid WKT geometry header"))?;
        parse_wkt_header(text, body_opened, start)
    }

    fn parse_body(
        &mut self,
        geometry_type: GeometryKind,
        axes: CoordinateAxes,
        axes_fixed: bool,
        depth: usize,
        header: &WktHeader,
    ) -> Result<Shape> {
        match geometry_type {
            GeometryKind::GeometryCollection => {
                self.parse_geometry_collection_body(depth, header, axes_fixed.then_some(axes))
            },
            GeometryKind::MultiPoint => {
                self.parse_multi_point_body(axes_fixed.then_some(axes), axes)
            },
            GeometryKind::MultiLineString => {
                self.parse_multi_line_body(axes_fixed.then_some(axes), axes)
            },
            GeometryKind::MultiPolygon => {
                self.parse_multi_polygon_body(axes_fixed.then_some(axes), axes)
            },
            GeometryKind::Point => {
                let axes = if axes_fixed {
                    axes
                } else {
                    self.infer_axes(false, false)?
                };
                let point = self.parse_coordinate(axes, false)?;
                let point = validate_wkt_point(point)?;
                Ok(Shape::Point(point))
            },
            GeometryKind::LineString => {
                let axes = if axes_fixed {
                    axes
                } else {
                    self.infer_axes(true, true)?
                };
                let coordinates = self.parse_coordinate_sequence(axes, true)?;
                Ok(Shape::LineString(LineSeq::try_new(coordinates)?))
            },
            GeometryKind::Polygon => {
                let mut polygon_axes = axes_fixed.then_some(axes);
                let polygon = self.parse_polygon_rings(&mut polygon_axes)?;
                Ok(Shape::Polygon(polygon))
            },
        }
    }

    fn parse_geometry_collection_body(
        &mut self,
        depth: usize,
        header: &WktHeader,
        inherited_axes: Option<CoordinateAxes>,
    ) -> Result<Shape> {
        let child_axes = if header.axes_explicit {
            Some(header.axes)
        } else {
            inherited_axes
        };
        if self.at_parenthesis_close() {
            return self
                .fail("empty parenthesized WKT member list; use EMPTY for an empty geometry");
        }

        let mut members = Vec::new();
        let mut first_error = None;
        let mut after_comma = false;
        loop {
            self.skip_whitespace();
            if self.at_parenthesis_close() {
                if after_comma {
                    return self.fail("trailing comma in geometry collection");
                }
                break;
            }
            if self.cursor.is_empty() {
                return self.fail("WKT is missing closing parenthesis");
            }
            if !self
                .cursor
                .peek()
                .is_some_and(|byte| byte.is_ascii_alphabetic())
            {
                return self.fail("expected geometry type in geometry collection member list");
            }

            let member_depth = self.parenthesis_depth;
            match self.parse_geometry(depth.wrapping_add(1), child_axes, ParentScope::Collection) {
                Ok(member) => members.push(member),
                Err(error) => {
                    remember_first(&mut first_error, error);
                    self.recover_collection_member(member_depth);
                },
            }

            self.skip_whitespace();
            match self.cursor.peek() {
                Some(b',') => {
                    self.cursor.advance();
                    after_comma = true;
                },
                Some(b')') => break,
                None => return self.fail("WKT is missing closing parenthesis"),
                Some(_) => {
                    self.recover_collection_member(member_depth);
                    match self.cursor.peek() {
                        Some(b',') => {
                            self.cursor.advance();
                            after_comma = true;
                        },
                        Some(b')') => break,
                        None => return self.fail("WKT is missing closing parenthesis"),
                        Some(_) => {
                            return self
                                .fail("expected geometry type in geometry collection member list");
                        },
                    }
                },
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(Shape::geometry_collection(members)?)
    }

    fn parse_multi_point_body(
        &mut self,
        mut axes: Option<CoordinateAxes>,
        default_axes: CoordinateAxes,
    ) -> Result<Shape> {
        if !self.is_wkt_member_list() {
            let axes = if let Some(axes) = axes {
                axes
            } else {
                self.infer_axes(true, true)?
            };
            let coordinates = self.parse_coordinate_sequence(axes, true)?;
            return Ok(Shape::MultiPoint(MultiPoint::from_coordseq(coordinates)?));
        }
        if self.at_parenthesis_close() {
            return self
                .fail("empty parenthesized WKT member list; use EMPTY for an empty geometry");
        }

        let mut members = Vec::new();
        let mut first_error = None;
        let mut inference_error = None;
        let mut after_comma = false;
        loop {
            self.skip_whitespace();
            if self.at_parenthesis_close() {
                if after_comma {
                    return self.fail("trailing comma in WKT member list");
                }
                break;
            }
            if self.cursor.is_empty() {
                return self.fail("WKT is missing closing parenthesis");
            }

            if self.is_wkt_empty_keyword() {
                self.cursor.advance_by(5);
                members.push(MultiMember::Empty);
            } else if self.cursor.peek() == Some(b'(') {
                self.open_parenthesis();
                let group_depth = self.parenthesis_depth;
                if axes.is_none() {
                    match self.infer_axes(false, false) {
                        Ok(inferred) => axes = Some(inferred),
                        Err(error) => {
                            remember_first(&mut inference_error, error);
                            axes = Some(CoordinateAxes::XY);
                            if !self.recover_to_group_close(group_depth) {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                            self.finish_member_separator(&mut after_comma)?;
                            continue;
                        },
                    }
                }
                let point_axes = axes.unwrap_or(default_axes);
                match self.parse_coordinate(point_axes, false) {
                    Ok(point) => {
                        if let Err(error) = self.close_parenthesis() {
                            remember_first(&mut first_error, error);
                            if !self.recover_to_group_close(group_depth) {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                        } else {
                            match validate_wkt_point(point) {
                                Ok(point) => members.push(MultiMember::Value(point)),
                                Err(error) => {
                                    remember_first(&mut first_error, self.position_error(error));
                                },
                            }
                        }
                    },
                    Err(error) => {
                        remember_first(&mut first_error, error);
                        if !self.recover_to_group_close(group_depth) {
                            return self.fail("WKT is missing closing parenthesis");
                        }
                    },
                }
            } else {
                return self.fail("expected WKT member (parenthesized group or EMPTY)");
            }

            self.finish_member_separator(&mut after_comma)?;
        }

        if let Some(error) = inference_error {
            return Err(error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(Shape::MultiPoint(MultiPoint::try_new(
            axes.unwrap_or(default_axes),
            members,
        )?))
    }

    fn parse_multi_line_body(
        &mut self,
        mut axes: Option<CoordinateAxes>,
        default_axes: CoordinateAxes,
    ) -> Result<Shape> {
        if self.at_parenthesis_close() {
            return self
                .fail("empty parenthesized WKT member list; use EMPTY for an empty geometry");
        }

        let mut members = Vec::new();
        let mut first_error = None;
        let mut inference_error = None;
        let mut after_comma = false;
        loop {
            self.skip_whitespace();
            if self.at_parenthesis_close() {
                if after_comma {
                    return self.fail("trailing comma in WKT member list");
                }
                break;
            }
            if self.cursor.is_empty() {
                return self.fail("WKT is missing closing parenthesis");
            }

            if self.is_wkt_empty_keyword() {
                self.cursor.advance_by(5);
                members.push(MultiMember::Empty);
            } else if self.cursor.peek() == Some(b'(') {
                self.open_parenthesis();
                let group_depth = self.parenthesis_depth;
                if axes.is_none() {
                    match self.infer_axes(true, true) {
                        Ok(inferred) => axes = Some(inferred),
                        Err(error) => {
                            remember_first(&mut inference_error, error);
                            axes = Some(CoordinateAxes::XY);
                            if !self.recover_to_group_close(group_depth) {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                            self.finish_member_separator(&mut after_comma)?;
                            continue;
                        },
                    }
                }
                let line_axes = axes.unwrap_or(default_axes);
                match self.parse_coordinate_sequence(line_axes, false) {
                    Ok(coordinates) => match self.close_parenthesis() {
                        Ok(()) => match LineSeq::try_new(coordinates) {
                            Ok(line) => members.push(MultiMember::Value(line)),
                            Err(error) => {
                                remember_first(&mut first_error, self.position_error(error));
                            },
                        },
                        Err(error) => {
                            remember_first(&mut first_error, error);
                            if !self.recover_to_group_close(group_depth) {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                        },
                    },
                    Err(error) => {
                        remember_first(&mut first_error, error);
                        if !self.recover_to_group_close(group_depth) {
                            return self.fail("WKT is missing closing parenthesis");
                        }
                    },
                }
            } else {
                return self.fail("expected WKT member (parenthesized group or EMPTY)");
            }

            self.finish_member_separator(&mut after_comma)?;
        }

        if let Some(error) = inference_error {
            return Err(error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(Shape::MultiLineString(MultiLineString::try_new(
            axes.unwrap_or(default_axes),
            members,
        )?))
    }

    fn parse_multi_polygon_body(
        &mut self,
        mut axes: Option<CoordinateAxes>,
        default_axes: CoordinateAxes,
    ) -> Result<Shape> {
        if self.at_parenthesis_close() {
            return self
                .fail("empty parenthesized WKT member list; use EMPTY for an empty geometry");
        }

        let mut members = Vec::new();
        let mut first_error = None;
        let mut inference_error = None;
        let mut after_comma = false;
        loop {
            self.skip_whitespace();
            if self.at_parenthesis_close() {
                if after_comma {
                    return self.fail("trailing comma in WKT member list");
                }
                break;
            }
            if self.cursor.is_empty() {
                return self.fail("WKT is missing closing parenthesis");
            }

            if self.is_wkt_empty_keyword() {
                self.cursor.advance_by(5);
                members.push(MultiMember::Empty);
            } else if self.cursor.peek() == Some(b'(') {
                self.open_parenthesis();
                let group_depth = self.parenthesis_depth;
                let axes_before = axes;
                match self.parse_polygon_rings(&mut axes) {
                    Ok(polygon) => match self.close_parenthesis() {
                        Ok(()) => members.push(MultiMember::Value(polygon)),
                        Err(error) => {
                            remember_first(&mut first_error, error);
                            if !self.recover_to_group_close(group_depth) {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                        },
                    },
                    Err(error) => {
                        if axes_before.is_none() && axes.is_none() {
                            remember_first(&mut inference_error, error);
                            axes = Some(CoordinateAxes::XY);
                        } else {
                            remember_first(&mut first_error, error);
                        }
                        if !self.recover_to_group_close(group_depth) {
                            return self.fail("WKT is missing closing parenthesis");
                        }
                    },
                }
            } else {
                return self.fail("expected WKT member (parenthesized group or EMPTY)");
            }

            self.finish_member_separator(&mut after_comma)?;
        }

        if let Some(error) = inference_error {
            return Err(error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(Shape::MultiPolygon(MultiPolygon::try_new(
            axes.unwrap_or(default_axes),
            members,
        )?))
    }

    fn parse_polygon_rings(&mut self, axes: &mut Option<CoordinateAxes>) -> Result<Polygon> {
        if self.at_parenthesis_close() {
            return self
                .fail("empty parenthesized WKT member list; use EMPTY for an empty geometry");
        }

        let mut rings = Vec::new();
        let mut holes = Vec::new();
        let mut first_error = None;
        let mut inference_error = None;
        let mut saw_group = false;
        let mut empty_before_axes_position = None;
        let mut after_comma = false;
        loop {
            self.skip_whitespace();
            if self.at_parenthesis_close() {
                if after_comma {
                    return self.fail("trailing comma in WKT member list");
                }
                break;
            }
            if self.cursor.is_empty() {
                return self.fail("WKT is missing closing parenthesis");
            }

            if self.is_wkt_empty_keyword() {
                let empty_position = self.cursor.position;
                self.cursor.advance_by(5);
                if axes.is_some() {
                    remember_first(
                        &mut first_error,
                        self.error_at(empty_position, "POLYGON ring cannot be EMPTY"),
                    );
                } else {
                    empty_before_axes_position.get_or_insert(empty_position);
                }
            } else if self.cursor.peek() == Some(b'(') {
                saw_group = true;
                self.open_parenthesis();
                let ring_depth = self.parenthesis_depth;
                if axes.is_none() {
                    match self.infer_axes(true, true) {
                        Ok(inferred) => {
                            *axes = Some(inferred);
                            if let Some(empty_position) = empty_before_axes_position {
                                remember_first(
                                    &mut first_error,
                                    self.error_at(empty_position, "POLYGON ring cannot be EMPTY"),
                                );
                            }
                        },
                        Err(error) => {
                            remember_first(&mut inference_error, error);
                            *axes = Some(CoordinateAxes::XY);
                            if !self.recover_to_group_close(ring_depth) {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                            self.finish_member_separator(&mut after_comma)?;
                            continue;
                        },
                    }
                }
                if inference_error.is_some() {
                    if !self.recover_to_group_close(ring_depth) {
                        return self.fail("WKT is missing closing parenthesis");
                    }
                    self.finish_member_separator(&mut after_comma)?;
                    continue;
                }
                let ring_axes = axes.unwrap_or(CoordinateAxes::XY);
                match self.parse_coordinate_sequence(ring_axes, false) {
                    Ok(coordinates) => match self.close_parenthesis() {
                        Ok(()) => match crate::io::admit_closed_ring(coordinates) {
                            Ok(ring) => {
                                if rings.is_empty() {
                                    rings.push(ring);
                                } else {
                                    holes.push(ring);
                                }
                            },
                            Err(error) => {
                                remember_first(&mut first_error, self.position_error(error));
                            },
                        },
                        Err(error) => {
                            remember_first(&mut first_error, error);
                            if !self.recover_to_group_close(ring_depth) {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                        },
                    },
                    Err(error) => {
                        remember_first(&mut first_error, error);
                        if !self.recover_to_group_close(ring_depth) {
                            return self.fail("WKT is missing closing parenthesis");
                        }
                    },
                }
            } else {
                return self.fail("expected WKT member (parenthesized group or EMPTY)");
            }

            self.finish_member_separator(&mut after_comma)?;
        }

        if let Some(error) = inference_error {
            return Err(error);
        }
        if !saw_group {
            return self.fail("POLYGON requires a shell");
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        let Some(shell) = rings.into_iter().next() else {
            return self.fail("POLYGON requires a shell");
        };
        Ok(Polygon::new(shell, holes))
    }

    fn parse_coordinate_sequence(
        &mut self,
        axes: CoordinateAxes,
        reserve_from_input_tail: bool,
    ) -> Result<CoordSeq> {
        crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
            self.parse_coordinate_sequence_axes::<HAS_Z, HAS_M>(reserve_from_input_tail)
        })
    }

    fn parse_coordinate_sequence_axes<const Z: bool, const M: bool>(
        &mut self,
        reserve_from_input_tail: bool,
    ) -> Result<CoordSeq> {
        let axes = CoordinateAxes::new(HasZ(Z), HasM(M));
        let first_start = self.cursor.position;
        let first = read_wkt_coordinate_axes!(self, Z, M, true);
        let first_end = self.cursor.position;
        let stride = first_end
            .saturating_sub(first_start)
            .saturating_add(2)
            .max(1);
        let capacity = if reserve_from_input_tail {
            self.cursor.bytes.len().saturating_sub(first_start) / stride
        } else {
            1
        };
        let mut builder = CoordSeqBuilder::with_capacity(axes, capacity);
        builder.push_xyzm(first.0, first.1, first.2, first.3);

        loop {
            skip_wkt_whitespace!(self);
            match self.cursor.peek() {
                Some(b',') => {
                    self.cursor.advance();
                    let coordinate = read_wkt_coordinate_axes!(self, Z, M, true);
                    builder.push_xyzm(coordinate.0, coordinate.1, coordinate.2, coordinate.3);
                },
                Some(b')') | None => break,
                Some(_) => {
                    return self.fail("coordinate has too many ordinates");
                },
            }
        }
        builder.finish().map_err(|error| self.position_error(error))
    }

    fn parse_coordinate(&mut self, axes: CoordinateAxes, comma_delimiter: bool) -> Result<Point> {
        crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
            self.parse_coordinate_axes::<HAS_Z, HAS_M>(comma_delimiter)
        })
    }

    fn parse_coordinate_axes<const Z: bool, const M: bool>(
        &mut self,
        comma_delimiter: bool,
    ) -> Result<Point> {
        let (x, y, z, m) = read_wkt_coordinate_axes!(self, Z, M, comma_delimiter);
        self.validate_coordinate_end(comma_delimiter)?;
        Ok(Point::new_raw_axes(x, y, ZOrdinate(z), MOrdinate(m)))
    }

    fn validate_coordinate_end(&mut self, comma_delimiter: bool) -> Result<()> {
        skip_wkt_whitespace!(self);
        if self
            .cursor
            .peek()
            .is_some_and(|byte| byte != b')' && (!comma_delimiter || byte != b','))
        {
            return self.fail("coordinate has too many ordinates");
        }
        Ok(())
    }

    fn infer_axes(
        &self,
        comma_delimiter: bool,
        empty_means_missing_x: bool,
    ) -> Result<CoordinateAxes> {
        let start = self.cursor.position;
        let end = self.coordinate_group_end(start, comma_delimiter);
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

    fn coordinate_group_end(&self, start: usize, comma_delimiter: bool) -> usize {
        let bytes = self.cursor.bytes;
        let mut position = start;
        let mut nested = 0_usize;
        while let Some(byte) = bytes.get(position).copied() {
            if comma_delimiter && byte == b',' {
                break;
            }
            if byte == b'(' {
                nested = nested.wrapping_add(1);
            } else if byte == b')' {
                if nested == 0 {
                    break;
                }
                nested = nested.wrapping_sub(1);
            }
            position = position.wrapping_add(1);
        }
        position
    }

    fn finish_member_separator(&mut self, after_comma: &mut bool) -> Result<()> {
        self.skip_whitespace();
        match self.cursor.peek() {
            Some(b',') => {
                self.cursor.advance();
                *after_comma = true;
                Ok(())
            },
            Some(b')') => {
                *after_comma = false;
                Ok(())
            },
            None => self.fail("WKT is missing closing parenthesis"),
            Some(_) => self.fail("expected comma between WKT members"),
        }
    }

    fn check_parent_boundary(&mut self, scope: ParentScope) -> Result<()> {
        self.skip_whitespace();
        match (scope, self.cursor.peek()) {
            (ParentScope::Root, None) => Ok(()),
            (ParentScope::Collection, None | Some(b',' | b')')) => Ok(()),
            (_, Some(_)) => self.fail("trailing text after WKT geometry"),
        }
    }

    fn is_wkt_member_list(&mut self) -> bool {
        self.skip_whitespace();
        self.cursor.peek() == Some(b'(') || self.is_wkt_empty_keyword()
    }

    fn is_wkt_empty_keyword(&mut self) -> bool {
        self.cursor
            .bytes
            .get(self.cursor.position..)
            .and_then(|tail| tail.get(..5))
            .is_some_and(|keyword| keyword.eq_ignore_ascii_case(b"EMPTY"))
            && !self
                .cursor
                .bytes
                .get(self.cursor.position.wrapping_add(5))
                .is_some_and(u8::is_ascii_alphanumeric)
    }

    fn at_parenthesis_close(&mut self) -> bool {
        self.skip_whitespace();
        self.cursor.peek() == Some(b')')
    }

    fn skip_whitespace(&mut self) {
        skip_wkt_whitespace!(self);
    }

    fn advance_char(&mut self) {
        let Some(byte) = self.cursor.peek() else {
            return;
        };
        if byte.is_ascii() {
            self.cursor.advance();
        } else if let Some(character) = self
            .input
            .get(self.cursor.position..)
            .and_then(|tail| tail.chars().next())
        {
            self.cursor.advance_by(character.len_utf8());
        }
    }

    fn open_parenthesis(&mut self) {
        self.cursor.advance();
        self.parenthesis_depth = self.parenthesis_depth.wrapping_add(1);
    }

    fn close_parenthesis(&mut self) -> Result<()> {
        self.skip_whitespace();
        if self.cursor.peek() != Some(b')') {
            return if self.cursor.is_empty() {
                self.fail("WKT is missing closing parenthesis")
            } else {
                self.fail("invalid WKT parentheses")
            };
        }
        self.cursor.advance();
        self.parenthesis_depth = self.parenthesis_depth.wrapping_sub(1);
        Ok(())
    }

    fn recover_to_geometry_close(&mut self, body_depth: usize) -> bool {
        let mut nested = self.parenthesis_depth.saturating_sub(body_depth);
        while let Some(byte) = self.cursor.peek() {
            match byte {
                b'(' => {
                    nested = nested.wrapping_add(1);
                    self.open_parenthesis();
                },
                b')' if nested == 0 => {
                    self.cursor.advance();
                    self.parenthesis_depth = self.parenthesis_depth.wrapping_sub(1);
                    return true;
                },
                b')' => {
                    nested = nested.wrapping_sub(1);
                    self.cursor.advance();
                    self.parenthesis_depth = self.parenthesis_depth.wrapping_sub(1);
                },
                _ => self.advance_char(),
            }
        }
        false
    }

    fn recover_to_group_close(&mut self, group_depth: usize) -> bool {
        self.recover_to_geometry_close(group_depth)
    }

    fn recover_collection_member(&mut self, base_depth: usize) {
        let mut nested = self.parenthesis_depth.saturating_sub(base_depth);
        while let Some(byte) = self.cursor.peek() {
            match byte {
                b'(' => {
                    nested = nested.wrapping_add(1);
                    self.open_parenthesis();
                },
                b')' if nested == 0 => break,
                b')' => {
                    nested = nested.wrapping_sub(1);
                    self.cursor.advance();
                    self.parenthesis_depth = self.parenthesis_depth.wrapping_sub(1);
                },
                b',' if nested == 0 => break,
                _ => self.advance_char(),
            }
        }
        debug_assert!(self.parenthesis_depth >= base_depth);
    }

    fn error_at(&self, position: usize, message: impl Into<Box<str>>) -> crate::error::Error {
        IoError::parse_at(ParseFormat::Wkt, position, message)
    }

    fn position_error(&self, error: crate::error::Error) -> crate::error::Error {
        match error.kind() {
            crate::error::ErrorKind::Geometry(_) => {
                IoError::parse_at(ParseFormat::Wkt, self.cursor.position, error.to_string())
            },
            crate::error::ErrorKind::Io(_) => error.with_parse_position(self.cursor.position),
            _ => error,
        }
    }

    fn fail<T>(&self, message: impl Into<Box<str>>) -> Result<T> {
        self.fail_at(self.cursor.position, message)
    }

    fn fail_at<T>(&self, position: usize, message: impl Into<Box<str>>) -> Result<T> {
        Err(self.error_at(position, message))
    }
}

fn validate_wkt_point(point: Point) -> Result<Point> {
    Point::new_axes(point.x, point.y, ZOrdinate(point.z()), MOrdinate(point.m()))
}

fn remember_first(slot: &mut Option<crate::error::Error>, error: crate::error::Error) {
    if slot.is_none() {
        *slot = Some(error);
    }
}

fn infer_wkt_axes_from_ordinate_count(count: usize) -> Result<CoordinateAxes> {
    match count {
        2 => Ok(CoordinateAxes::XY),
        3 => Ok(CoordinateAxes::XYZ),
        4 => Ok(CoordinateAxes::XYZM),
        0 | 1 => Err(IoError::wkt("coordinate is missing y")),
        _ => Err(IoError::wkt("coordinate has too many ordinates")),
    }
}

/// Resolve axes for a geometry that may sit under a tagged GeometryCollection.
fn resolve_wkt_member_axes(
    header: &WktHeader,
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

fn parse_wkt_type_keyword_upper(upper: &[u8]) -> Option<(GeometryKind, Option<CoordinateAxes>)> {
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

fn parse_wkt_header(source: &str, body_opened: bool, input_offset: usize) -> Result<WktHeader> {
    let leading_whitespace = source.len().saturating_sub(source.trim_start().len());
    let text = source.trim();
    let header_error = |position: usize, message: Box<str>| {
        IoError::parse_at(
            ParseFormat::Wkt,
            input_offset.wrapping_add(position),
            message,
        )
    };
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
    let mut rest_position =
        leading_whitespace + keyword_len + raw_rest.len().saturating_sub(rest.len());
    let (mut axes, mut axes_explicit) =
        compact_axes.map_or((CoordinateAxes::XY, false), |compact| (compact, true));
    let compact_axes_length = match compact_axes {
        Some(CoordinateAxes::XYZM) => 2,
        Some(_) => 1,
        None => 0,
    };
    let mut axes_position = compact_axes.map(|_| {
        input_offset
            .wrapping_add(leading_whitespace)
            .wrapping_add(keyword_len.saturating_sub(compact_axes_length))
    });
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
            axes_position = Some(input_offset.wrapping_add(rest_position));
            let after_axis = rest
                .get(axis_len..)
                .ok_or_else(|| header_error(rest_position, "invalid WKT dimensional tag".into()))?;
            rest = after_axis.trim_start();
            rest_position = rest_position
                .wrapping_add(axis_len)
                .wrapping_add(after_axis.len().saturating_sub(rest.len()));
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

struct WktByteCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> WktByteCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }

    const fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn advance(&mut self) {
        self.position = self.position.wrapping_add(1);
        #[cfg(test)]
        WKT_CURSOR_WORK.with(|work| work.set(work.get().wrapping_add(1)));
    }

    fn advance_by(&mut self, count: usize) {
        self.position = self.position.wrapping_add(count);
        #[cfg(test)]
        WKT_CURSOR_WORK.with(|work| work.set(work.get().wrapping_add(count)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinate_cursor_preserves_float_bits_and_ordinates() {
        let line = parse_wkt("LINESTRING (-0.0 1.2345678901234567, 1e308 -1e-308)")
            .expect("ASCII coordinates parse");
        let Shape::LineString(line) = line else {
            panic!("expected line string");
        };
        assert_eq!(line.as_coords().xs()[0].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(
            line.as_coords().ys()[0].to_bits(),
            1.234_567_890_123_456_7_f64.to_bits()
        );
        assert_eq!(line.as_coords().xs()[1].to_bits(), 1e308_f64.to_bits());
        assert_eq!(line.as_coords().ys()[1].to_bits(), (-1e-308_f64).to_bits());

        let point = parse_wkt("POINT ZM (1 2 3 4)").expect("tagged point parses");
        let Shape::Point(point) = point else {
            panic!("expected point");
        };
        assert_eq!(
            (point.x, point.y, point.z(), point.m()),
            (1.0, 2.0, Some(3.0), Some(4.0))
        );
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

    fn parse_wkt_counted(value: &str) -> (Shape, usize) {
        WKT_CURSOR_WORK.with(|work| work.set(0));
        let shape = parse_wkt(value).expect("counted WKT parses");
        let work = WKT_CURSOR_WORK.with(Cell::get);
        (shape, work)
    }

    #[test]
    fn nested_collections_advance_a_linear_number_of_cursor_bytes() {
        let mut input = "GEOMETRYCOLLECTION (".repeat(128);
        input.push_str("POINT (0.1 0.2)");
        input.push_str(&")".repeat(128));

        let (_, work) = parse_wkt_counted(&input);
        assert!(
            work <= input.len().saturating_mul(2),
            "cursor advanced {} times for {} UTF-8 bytes",
            work,
            input.len()
        );
    }

    #[test]
    fn fixed_size_nested_vertex_input_does_not_repeat_member_scans() {
        let depth = 128;
        let prefix = "GEOMETRYCOLLECTION (".repeat(depth);
        let suffix = ")".repeat(depth);
        let coordinates = "0.1 0.2, ".repeat(8191) + "0.1 0.2";
        let body_length = 131_072_usize;
        let fixed = prefix.len() + "LINESTRING (".len() + coordinates.len() + suffix.len() + 1;
        let input = format!(
            "{prefix}LINESTRING ({}{padding}){suffix}",
            coordinates,
            padding = " ".repeat(body_length.saturating_sub(fixed)),
        );

        let (_, work) = parse_wkt_counted(&input);
        assert!(
            work <= input.len().saturating_mul(2),
            "cursor advanced {} times for {} UTF-8 bytes",
            work,
            input.len()
        );
    }
}
