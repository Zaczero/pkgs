use crate::geometry::*;

/// Which operand of a rectangle-routed intersection is the rectangle.
#[derive(Clone, Copy)]
pub(crate) enum RectSide {
    Left,
    Right,
}

/// The operand as an axis-aligned rectangle: a hole-less 5-point shell
/// whose vertices are exactly its bounds' corners with axis-sharing
/// consecutive edges. `None` for everything else (degenerate rectangles
/// included). Exact `==` on purpose — only literal rectangles route.
#[expect(clippy::float_cmp)]
pub(crate) fn axis_rectangle(shape: &Shape) -> Option<Bounds> {
    let Shape::Polygon(polygon) = shape else {
        return None;
    };
    if !polygon.holes.is_empty() {
        return None;
    }
    let coords = polygon.shell.coords();
    if coords.coord_count() != 5 {
        return None;
    }
    let bounds = Bounds::from_coords(coords)?;
    if bounds.minx() >= bounds.maxx() || bounds.miny() >= bounds.maxy() {
        return None;
    }
    let (xs, ys) = (coords.xs(), coords.ys());
    for index in 0..4 {
        let corner_x = xs[index] == bounds.minx() || xs[index] == bounds.maxx();
        let corner_y = ys[index] == bounds.miny() || ys[index] == bounds.maxy();
        if !(corner_x && corner_y) {
            return None;
        }
        if xs[index] != xs[index + 1] && ys[index] != ys[index + 1] {
            return None;
        }
    }
    Some(bounds)
}

pub(crate) fn rect_polygon(rect: Bounds) -> Polygon {
    let shell = vec![
        Point::new_unchecked_xy(rect.minx(), rect.miny()),
        Point::new_unchecked_xy(rect.maxx(), rect.miny()),
        Point::new_unchecked_xy(rect.maxx(), rect.maxy()),
        Point::new_unchecked_xy(rect.minx(), rect.maxy()),
        Point::new_unchecked_xy(rect.minx(), rect.miny()),
    ];
    Polygon::new(Ring::from_trusted_closed(shell), Vec::new())
}
