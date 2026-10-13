use super::{
    CoordinateAxes, IoError, LineSeq, MultiLineString, MultiMember, MultiPoint, MultiPolygon,
    ParentScope, Polygon, Result, Shape, WktHeader, WktParser, remember_first, validate_wkt_point,
};

impl WktParser<'_> {
    pub(super) fn parse_geometry_collection_body(
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
        if self.at_parenthesis_close()? {
            return self
                .fail("empty parenthesized WKT member list; use EMPTY for an empty geometry");
        }

        let mut members = Vec::new();
        let mut first_error = None;
        let mut after_comma = false;
        loop {
            self.skip_whitespace()?;
            if self.at_parenthesis_close()? {
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
            let child_depth = depth
                .checked_add(1)
                .ok_or_else(|| self.error_at(self.position(), "WKT nesting depth overflow"))?;
            match self.parse_geometry(child_depth, child_axes, ParentScope::Collection) {
                Ok(member) => members.push(member),
                Err(error) => {
                    remember_first(&mut first_error, error);
                    self.recover_collection_member(member_depth)?;
                },
            }

            self.skip_whitespace()?;
            match self.cursor.peek() {
                Some(b',') => {
                    self.advance_cursor()?;
                    after_comma = true;
                },
                Some(b')') => break,
                None => return self.fail("WKT is missing closing parenthesis"),
                Some(_) => {
                    self.recover_collection_member(member_depth)?;
                    match self.cursor.peek() {
                        Some(b',') => {
                            self.advance_cursor()?;
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
        Shape::geometry_collection(members)
    }

    pub(super) fn parse_multi_point_body(
        &mut self,
        mut axes: Option<CoordinateAxes>,
        default_axes: CoordinateAxes,
    ) -> Result<Shape> {
        if !self.is_wkt_member_list()? {
            let axes = if let Some(axes) = axes {
                axes
            } else {
                self.infer_axes(true, true)?
            };
            let coordinates = self.parse_coordinate_sequence(axes, true)?;
            return Ok(Shape::MultiPoint(MultiPoint::from_coordseq(coordinates)?));
        }
        if self.at_parenthesis_close()? {
            return self
                .fail("empty parenthesized WKT member list; use EMPTY for an empty geometry");
        }

        let mut members = Vec::new();
        let mut first_error = None;
        let mut inference_error = None;
        let mut after_comma = false;
        loop {
            self.skip_whitespace()?;
            if self.at_parenthesis_close()? {
                if after_comma {
                    return self.fail("trailing comma in WKT member list");
                }
                break;
            }
            if self.cursor.is_empty() {
                return self.fail("WKT is missing closing parenthesis");
            }

            if self.is_wkt_empty_keyword() {
                self.advance_cursor_by(5)?;
                members.push(MultiMember::Empty);
            } else if self.cursor.peek() == Some(b'(') {
                self.open_parenthesis()?;
                let group_depth = self.parenthesis_depth;
                if axes.is_none() {
                    match self.infer_axes(false, false) {
                        Ok(inferred) => axes = Some(inferred),
                        Err(error) => {
                            remember_first(&mut inference_error, error);
                            axes = Some(CoordinateAxes::XY);
                            if !self.recover_to_group_close(group_depth)? {
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
                            if !self.recover_to_group_close(group_depth)? {
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
                        if !self.recover_to_group_close(group_depth)? {
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

    pub(super) fn parse_multi_line_body(
        &mut self,
        mut axes: Option<CoordinateAxes>,
        default_axes: CoordinateAxes,
    ) -> Result<Shape> {
        if self.at_parenthesis_close()? {
            return self
                .fail("empty parenthesized WKT member list; use EMPTY for an empty geometry");
        }

        let mut members = Vec::new();
        let mut first_error = None;
        let mut inference_error = None;
        let mut after_comma = false;
        loop {
            self.skip_whitespace()?;
            if self.at_parenthesis_close()? {
                if after_comma {
                    return self.fail("trailing comma in WKT member list");
                }
                break;
            }
            if self.cursor.is_empty() {
                return self.fail("WKT is missing closing parenthesis");
            }

            if self.is_wkt_empty_keyword() {
                self.advance_cursor_by(5)?;
                members.push(MultiMember::Empty);
            } else if self.cursor.peek() == Some(b'(') {
                self.open_parenthesis()?;
                let group_depth = self.parenthesis_depth;
                if axes.is_none() {
                    match self.infer_axes(true, true) {
                        Ok(inferred) => axes = Some(inferred),
                        Err(error) => {
                            remember_first(&mut inference_error, error);
                            axes = Some(CoordinateAxes::XY);
                            if !self.recover_to_group_close(group_depth)? {
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
                            if !self.recover_to_group_close(group_depth)? {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                        },
                    },
                    Err(error) => {
                        remember_first(&mut first_error, error);
                        if !self.recover_to_group_close(group_depth)? {
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

    pub(super) fn parse_multi_polygon_body(
        &mut self,
        mut axes: Option<CoordinateAxes>,
        default_axes: CoordinateAxes,
    ) -> Result<Shape> {
        if self.at_parenthesis_close()? {
            return self
                .fail("empty parenthesized WKT member list; use EMPTY for an empty geometry");
        }

        let mut members = Vec::new();
        let mut first_error = None;
        let mut inference_error = None;
        let mut after_comma = false;
        loop {
            self.skip_whitespace()?;
            if self.at_parenthesis_close()? {
                if after_comma {
                    return self.fail("trailing comma in WKT member list");
                }
                break;
            }
            if self.cursor.is_empty() {
                return self.fail("WKT is missing closing parenthesis");
            }

            if self.is_wkt_empty_keyword() {
                self.advance_cursor_by(5)?;
                members.push(MultiMember::Empty);
            } else if self.cursor.peek() == Some(b'(') {
                self.open_parenthesis()?;
                let group_depth = self.parenthesis_depth;
                let axes_before = axes;
                match self.parse_polygon_rings(&mut axes) {
                    Ok(polygon) => match self.close_parenthesis() {
                        Ok(()) => members.push(MultiMember::Value(polygon)),
                        Err(error) => {
                            remember_first(&mut first_error, error);
                            if !self.recover_to_group_close(group_depth)? {
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
                        if !self.recover_to_group_close(group_depth)? {
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

    pub(super) fn parse_polygon_rings(
        &mut self,
        axes: &mut Option<CoordinateAxes>,
    ) -> Result<Polygon> {
        if self.at_parenthesis_close()? {
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
            self.skip_whitespace()?;
            if self.at_parenthesis_close()? {
                if after_comma {
                    return self.fail("trailing comma in WKT member list");
                }
                break;
            }
            if self.cursor.is_empty() {
                return self.fail("WKT is missing closing parenthesis");
            }

            if self.is_wkt_empty_keyword() {
                let empty_position = self.position();
                self.advance_cursor_by(5)?;
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
                self.open_parenthesis()?;
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
                            if !self.recover_to_group_close(ring_depth)? {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                            self.finish_member_separator(&mut after_comma)?;
                            continue;
                        },
                    }
                }
                if inference_error.is_some() {
                    if !self.recover_to_group_close(ring_depth)? {
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
                            if !self.recover_to_group_close(ring_depth)? {
                                return self.fail("WKT is missing closing parenthesis");
                            }
                        },
                    },
                    Err(error) => {
                        remember_first(&mut first_error, error);
                        if !self.recover_to_group_close(ring_depth)? {
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
}

/// Resolve axes for a geometry that may sit under a tagged GeometryCollection.
pub(super) fn resolve_wkt_member_axes(
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
