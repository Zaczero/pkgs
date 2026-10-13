use super::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, HasM, HasZ, MOrdinate, Point, Result, WktParser,
    ZOrdinate,
};

macro_rules! parse_wkt_ordinate {
    ($parser:ident, $name:literal, $comma_delimiter:expr) => {{
        skip_wkt_whitespace!($parser);
        let value = if let Some(value) = $parser.take_ascii_ordinate($comma_delimiter) {
            value
        } else {
            let remaining = $parser.cursor.remaining;
            while let Some(byte) = $parser.cursor.peek() {
                if byte == b')' || ($comma_delimiter && byte == b',') || byte.is_ascii_whitespace()
                {
                    break;
                }
                if byte == b'(' {
                    $parser.open_parenthesis()?;
                } else if byte.is_ascii() {
                    $parser.advance_cursor()?;
                } else {
                    let character = $parser
                        .input
                        .get($parser.position()..)
                        .and_then(|tail| tail.chars().next())
                        .ok_or_else(|| {
                            $parser.error_at($parser.position(), "invalid WKT coordinate")
                        })?;
                    if character.is_whitespace() {
                        break;
                    }
                    $parser.advance_cursor_by(character.len_utf8())?;
                }
            }
            let token_length = remaining
                .len()
                .checked_sub($parser.cursor.remaining.len())
                .ok_or_else(|| $parser.error_at($parser.position(), "invalid WKT coordinate"))?;
            let token = remaining
                .get(..token_length)
                .ok_or_else(|| $parser.error_at($parser.position(), "invalid WKT coordinate"))?;
            if token.is_empty() {
                return $parser.fail(format!("coordinate is missing {}", $name));
            }
            match lexical_core::parse_partial::<f64>(token) {
                Ok((value, parsed_length)) if parsed_length == token_length => value,
                _ => {
                    let end = $parser.position();
                    let start = end
                        .checked_sub(token_length)
                        .ok_or_else(|| $parser.error_at(end, "invalid WKT coordinate"))?;
                    return $parser.fail_at(start, format!("{} coordinate must be numeric", $name));
                },
            }
        };
        value
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

impl WktParser<'_> {
    pub(super) fn parse_coordinate_sequence(
        &mut self,
        axes: CoordinateAxes,
        reserve_from_input_tail: bool,
    ) -> Result<CoordSeq> {
        crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
            self.parse_coordinate_sequence_axes::<HAS_Z, HAS_M>(reserve_from_input_tail)
        })
    }

    pub(super) fn parse_coordinate_sequence_axes<const Z: bool, const M: bool>(
        &mut self,
        reserve_from_input_tail: bool,
    ) -> Result<CoordSeq> {
        let axes = CoordinateAxes::new(HasZ(Z), HasM(M));
        let first_start = self.position();
        let remaining_bytes = self.cursor.remaining.len();
        let first = read_wkt_coordinate_axes!(self, Z, M, true);
        let first_bytes = remaining_bytes
            .checked_sub(self.cursor.remaining.len())
            .ok_or_else(|| self.error_at(first_start, "invalid WKT coordinate"))?;
        let stride = first_bytes
            .checked_add(2)
            .map(|stride| stride.max(1))
            .ok_or_else(|| self.error_at(first_start, "WKT coordinate size overflow"))?;
        let capacity = if reserve_from_input_tail {
            remaining_bytes
                .checked_div(stride)
                .ok_or_else(|| self.error_at(first_start, "WKT coordinate size overflow"))?
        } else {
            1
        };
        let mut builder = CoordSeqBuilder::with_capacity(axes, capacity);
        builder.push_xyzm(first.0, first.1, first.2, first.3);

        loop {
            skip_wkt_whitespace!(self);
            match self.cursor.peek() {
                Some(b',') => {
                    self.advance_cursor()?;
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

    pub(super) fn parse_coordinate(
        &mut self,
        axes: CoordinateAxes,
        comma_delimiter: bool,
    ) -> Result<Point> {
        crate::geometry::dispatch_axes!((axes.has_z(), axes.has_m()), HAS_Z, HAS_M, {
            self.parse_coordinate_axes::<HAS_Z, HAS_M>(comma_delimiter)
        })
    }

    pub(super) fn parse_coordinate_axes<const Z: bool, const M: bool>(
        &mut self,
        comma_delimiter: bool,
    ) -> Result<Point> {
        let (x, y, z, m) = read_wkt_coordinate_axes!(self, Z, M, comma_delimiter);
        self.validate_coordinate_end(comma_delimiter)?;
        Ok(Point::new_raw_axes(x, y, ZOrdinate(z), MOrdinate(m)))
    }

    pub(super) fn take_ascii_ordinate(&mut self, comma_delimiter: bool) -> Option<f64> {
        let remaining = self.cursor.remaining;
        let (value, length) = lexical_core::parse_partial::<f64>(remaining).ok()?;
        if length == 0 {
            return None;
        }
        let tail = remaining.get(length..)?;
        if let Some(&byte) = tail.first() {
            if byte.is_ascii() {
                if byte != b')' && (!comma_delimiter || byte != b',') && !byte.is_ascii_whitespace()
                {
                    return None;
                }
            } else {
                let position = self.position().checked_add(length)?;
                if !self.input.get(position..)?.chars().next()?.is_whitespace() {
                    return None;
                }
            }
        }
        self.cursor.advance_by(length).then_some(value)
    }

    pub(super) fn validate_coordinate_end(&mut self, comma_delimiter: bool) -> Result<()> {
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
}

pub(super) fn validate_wkt_point(point: Point) -> Result<Point> {
    Point::new_axes(point.x, point.y, ZOrdinate(point.z()), MOrdinate(point.m()))
}
