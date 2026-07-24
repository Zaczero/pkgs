use crate::geometry::relate::{line_has_nonzero_segment, multiline_has_nonzero_segment};
use crate::geometry::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DimMode {
    Topology,
    Effective,
    Declared,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Dimension {
    Point = 0,
    Curve = 1,
    Surface = 2,
}

impl Dimension {
    pub(crate) const fn code(self) -> u8 {
        self as u8
    }
}

impl From<Dimension> for u8 {
    fn from(value: Dimension) -> Self {
        value.code()
    }
}

impl From<Dimension> for usize {
    fn from(value: Dimension) -> Self {
        Self::from(value.code())
    }
}

pub(crate) fn dimension(shape: &Shape, mode: DimMode) -> Option<Dimension> {
    match shape {
        Shape::Empty(kind, _) => match mode {
            DimMode::Declared => match kind {
                EmptyKind::Point => Some(Dimension::Point),
                EmptyKind::MultiLineString => Some(Dimension::Curve),
                EmptyKind::Polygon | EmptyKind::MultiPolygon => Some(Dimension::Surface),
                // Matches the empty container form: `max` over no members.
                EmptyKind::GeometryCollection => None,
            },
            DimMode::Topology | DimMode::Effective => None,
        },
        Shape::Point(_) => Some(Dimension::Point),
        Shape::MultiPoint(points) => match mode {
            DimMode::Topology => (points.coord_count() > 0).then_some(Dimension::Point),
            DimMode::Effective => (!points.is_empty()).then_some(Dimension::Point),
            DimMode::Declared => Some(Dimension::Point),
        },
        Shape::LineString(points) => match mode {
            DimMode::Topology => (points.coord_count() > 0).then_some(Dimension::Curve),
            DimMode::Effective => {
                if points.is_empty() {
                    None
                } else if line_has_nonzero_segment(points) {
                    Some(Dimension::Curve)
                } else {
                    Some(Dimension::Point)
                }
            },
            DimMode::Declared => Some(Dimension::Curve),
        },
        Shape::MultiLineString(lines) => match mode {
            DimMode::Topology => lines
                .iter()
                .any(|line| line.coord_count() > 0)
                .then_some(Dimension::Curve),
            DimMode::Effective => {
                if multiline_has_nonzero_segment(lines) {
                    Some(Dimension::Curve)
                } else {
                    lines
                        .iter()
                        .any(|line| !line.is_empty())
                        .then_some(Dimension::Point)
                }
            },
            DimMode::Declared => Some(Dimension::Curve),
        },
        Shape::Polygon(_) => Some(Dimension::Surface),
        Shape::MultiPolygon(polygons) => match mode {
            DimMode::Declared => Some(Dimension::Surface),
            DimMode::Topology | DimMode::Effective => {
                (!polygons.is_empty()).then_some(Dimension::Surface)
            },
        },
        Shape::GeometryCollection(parts) => {
            parts.iter().filter_map(|part| dimension(part, mode)).max()
        },
    }
}
