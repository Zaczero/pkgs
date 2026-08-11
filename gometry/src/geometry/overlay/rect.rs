use crate::geometry::{Bounds, Coordinates as _, Point, Polygon, Ring, Shape};

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
pub(crate) fn axis_rectangle(shape: &Shape) -> Option<Bounds> {
    let Shape::Polygon(polygon) = shape else {
        return None;
    };
    axis_rectangle_polygon(polygon)
}

#[expect(
    clippy::float_cmp,
    reason = "only literal stored-coordinate rectangles use this exact topology fast path"
)]
pub(crate) fn axis_rectangle_polygon(polygon: &Polygon) -> Option<Bounds> {
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
    let mut corner_mask = 0_u8;
    for index in 0..4 {
        let corner_x = if xs[index] == bounds.minx() {
            0
        } else if xs[index] == bounds.maxx() {
            1
        } else {
            return None;
        };
        let corner_y = if ys[index] == bounds.miny() {
            0
        } else if ys[index] == bounds.maxy() {
            1
        } else {
            return None;
        };
        corner_mask |= 1 << (corner_x | (corner_y << 1));
        if xs[index] != xs[index + 1] && ys[index] != ys[index + 1] {
            return None;
        }
    }
    (corner_mask == 0b1111).then_some(bounds)
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

/// The positive-area intersection of two axis-aligned rectangles.
///
/// This is deliberately narrower than the general rectangle route: a touch
/// still goes through the full overlay so its 0-D/1-D result is preserved.
/// For a proper areal overlap, however, choosing each edge directly from the
/// stored bounds avoids an affine clip fraction.  That fraction is not
/// representable for subnormal and extreme finite coordinates, while
/// `max`/`min` are exact selectors of an input double.
pub(crate) fn proper_rect_intersection(left: Bounds, right: Bounds) -> Option<Bounds> {
    let minx = left.minx().max(right.minx());
    let miny = left.miny().max(right.miny());
    let maxx = left.maxx().min(right.maxx());
    let maxy = left.maxy().min(right.maxy());
    (minx < maxx && miny < maxy).then(|| Bounds::new_unchecked(minx, miny, maxx, maxy))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn polygon(coords: &[(f64, f64)]) -> Polygon {
        Polygon::new(
            Ring::from_trusted_closed(
                coords
                    .iter()
                    .map(|&(x, y)| Point::new_unchecked_xy(x, y))
                    .collect::<Vec<_>>(),
            ),
            Vec::new(),
        )
    }

    #[test]
    fn rectangle_certificate_requires_every_corner_and_keeps_literal_rectangles() {
        for mut cycle in [vec![(0.0, 0.0), (2.0, 0.0), (2.0, 1.0), (0.0, 1.0)], vec![
            (0.0, 0.0),
            (0.0, 1.0),
            (2.0, 1.0),
            (2.0, 0.0),
        ]] {
            for _ in 0..4 {
                let mut closed = cycle.clone();
                closed.push(cycle[0]);
                assert!(axis_rectangle_polygon(&polygon(&closed)).is_some());
                cycle.rotate_left(1);
            }
        }
        for coords in [
            [(0.0, 0.0), (1.0, 0.0), (0.0, 0.0), (0.0, 1.0), (0.0, 0.0)],
            [(0.0, 0.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (0.0, 0.0)],
            [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0), (0.0, 0.0)],
            [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (1.0, 1.0), (0.0, 0.0)],
            [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (3.0, 0.0), (0.0, 0.0)],
        ] {
            assert!(axis_rectangle_polygon(&polygon(&coords)).is_none());
        }
    }
}
