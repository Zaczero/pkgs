use crate::geometry::Shape;
use crate::geometry::relate::{line_has_nonzero_segment, multiline_has_nonzero_segment};

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

pub(crate) fn dimension(shape: &Shape) -> Option<Dimension> {
    match shape {
        Shape::Empty(..) => None,
        Shape::Point(_) => Some(Dimension::Point),
        Shape::MultiPoint(points) => (!points.is_empty()).then_some(Dimension::Point),
        Shape::LineString(points) => {
            if points.is_empty() {
                None
            } else if line_has_nonzero_segment(points) {
                Some(Dimension::Curve)
            } else {
                Some(Dimension::Point)
            }
        },
        Shape::MultiLineString(lines) => {
            if multiline_has_nonzero_segment(lines) {
                Some(Dimension::Curve)
            } else {
                lines
                    .iter()
                    .any(|line| !line.is_empty())
                    .then_some(Dimension::Point)
            }
        },
        Shape::Polygon(_) => Some(Dimension::Surface),
        Shape::MultiPolygon(polygons) => (!polygons.is_empty()).then_some(Dimension::Surface),
        Shape::GeometryCollection(parts) => parts.iter().filter_map(dimension).max(),
    }
}
