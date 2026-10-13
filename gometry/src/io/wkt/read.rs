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

use cursor::WktByteCursor;

pub(crate) fn parse_wkt(value: &str) -> Result<Shape> {
    let mut parser = WktParser::new(value);
    parser
        .parse_document()
        .map_err(|error| parse_content(ParseFormat::Wkt, error))
        .map_err(|error| error.with_parse_position(parser.position()))
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
    const fn new(input: &'a str) -> Self {
        Self {
            input,
            cursor: WktByteCursor::new(input.as_bytes()),
            parenthesis_depth: 0,
        }
    }

    const fn position(&self) -> usize {
        self.input
            .len()
            .checked_sub(self.cursor.remaining.len())
            .expect("WKT cursor remains a suffix of its input")
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
                let position = header.axes_position.unwrap_or_else(|| self.position());
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
                if !self.recover_to_geometry_close(body_depth)? {
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
        let start = self.position();
        let mut body_opened = false;
        while let Some(byte) = self.cursor.peek() {
            if byte == b'(' {
                self.open_parenthesis()?;
                body_opened = true;
                break;
            }
            if matches!(parent_scope, ParentScope::Collection) && matches!(byte, b',' | b')') {
                break;
            }
            self.advance_char()?;
        }

        let text = self
            .input
            .get(start..self.position())
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
}

#[macro_use]
mod cursor;

mod coordinates;

mod collections;
mod header;
#[cfg(test)]
mod tests;

use collections::resolve_wkt_member_axes;
use coordinates::validate_wkt_point;
use cursor::remember_first;
pub(crate) use header::classify_wkt_type_token;
use header::parse_wkt_header;
