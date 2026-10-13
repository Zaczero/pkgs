use crate::geometry::{CoordSeq, Coordinates as _, Shape, same_point};

fn line_has_nonzero_segment(coords: &CoordSeq) -> bool {
    coords
        .segment_pairs()
        .any(|[start, end]| !same_point(start, end))
}

fn multiline_has_nonzero_segment<L: AsRef<CoordSeq>>(lines: &[L]) -> bool {
    lines
        .iter()
        .any(|line| line_has_nonzero_segment(line.as_ref()))
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

/// Whether every vertex of a ring lies on one line, decided exactly.
#[expect(
    clippy::float_cmp,
    reason = "the first vertex distinct from the origin is found by identity, not proximity; a tolerance would pick a near-duplicate as the line's second point"
)]
pub(crate) fn ring_is_collinear(coords: &(impl crate::geometry::Coordinates + ?Sized)) -> bool {
    let (xs, ys) = coords.xy_columns().expect("rings carry coordinate columns");
    let Some((&ax, &ay)) = xs.first().zip(ys.first()) else {
        return true;
    };
    // The first vertex distinct from the origin fixes the candidate line.
    let Some((bx, by)) = xs
        .iter()
        .copied()
        .zip(ys.iter().copied())
        .find(|&(x, y)| x != ax || y != ay)
    else {
        return true;
    };
    xs.iter().copied().zip(ys.iter().copied()).all(|(x, y)| {
        crate::geometry::orientation_xy(ax, ay, bx, by, x, y)
            == crate::geometry::Orientation::Collinear
    })
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
