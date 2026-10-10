use std::cmp::Ordering;

use crate::geometry::{Orientation, XY, orientation};

/// Counter-clockwise order starting at the positive x axis. Quadrants first
/// restrict the certified orientation to an angle smaller than pi, making its
/// sign transitive. Collinear rays are then an equivalence class, ordered by
/// target solely to make the comparator total.
pub(crate) fn compare_departures(
    origin: XY,
    left: XY,
    left_target: u32,
    right: XY,
    right_target: u32,
) -> Ordering {
    let quadrant = |point: XY| match (point.y >= origin.y, point.x >= origin.x) {
        (true, true) => 0,
        (true, false) => 1,
        (false, false) => 2,
        (false, true) => 3,
    };
    quadrant(left)
        .cmp(&quadrant(right))
        .then_with(|| match orientation(origin, left, right) {
            Orientation::CounterClockwise => Ordering::Less,
            Orientation::Clockwise => Ordering::Greater,
            Orientation::Collinear => left_target.cmp(&right_target),
        })
}
