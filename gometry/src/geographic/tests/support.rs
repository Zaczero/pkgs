use super::*;

pub(super) fn factor(value: f64) -> AngularFactor {
    AngularFactor::try_new("unit", value).unwrap()
}
pub(super) fn frame(value: f64) -> GeographicFrame {
    GeographicFrame::try_new(factor(value), factor(value), 0.0).unwrap()
}
pub(super) fn degrees() -> GeographicFrame {
    frame(std::f64::consts::PI / 180.0)
}
pub(super) fn assert_bits(value: f64, expected: f64) {
    assert_eq!(value.to_bits(), expected.to_bits());
}
pub(super) fn xy_seq(xs: Vec<f64>, ys: Vec<f64>) -> crate::geometry::CoordSeq {
    crate::geometry::CoordSeq::try_from_vecs(xs, ys, None, None).unwrap()
}
pub(super) fn ring(xs: Vec<f64>, ys: Vec<f64>) -> crate::geometry::Ring {
    crate::geometry::Ring::closed_coordseq(xy_seq(xs, ys)).unwrap()
}
pub(super) fn ring_from_points(points: &[(f64, f64)]) -> crate::geometry::Ring {
    ring(
        points.iter().map(|&(x, _)| x).collect(),
        points.iter().map(|&(_, y)| y).collect(),
    )
}
pub(super) fn nested_member_path(
    path: &[usize],
    mut child: crate::geometry::Shape,
) -> crate::geometry::Shape {
    use crate::geometry::{CoordinateAxes, GeometryKind, Shape};

    for &member in path.iter().rev() {
        let mut members = vec![Shape::Empty(GeometryKind::Point, CoordinateAxes::XY); member];
        members.push(child);
        child = Shape::geometry_collection(members).expect("test member path stays within limits");
    }
    child
}
pub(super) fn invalid_hole_polygon() -> crate::geometry::Polygon {
    use crate::geometry::Polygon;

    let shell = ring_from_points(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 0.0)]);
    let hole = ring_from_points(&[(0.5, 0.5), (1.5, 0.5), (1.5, 91.0), (0.5, 0.5)]);
    Polygon::new(shell, vec![hole])
}
pub(super) fn nested_window_ring(ys: Vec<f64>) -> (crate::geometry::Ring, usize) {
    use crate::geometry::{CoordWindow, Ring};

    let parent = xy_seq(vec![901.0, 902.0, 10.0, 20.0, 20.0, 10.0, 903.0], ys);
    let parent_window = CoordWindow::checked(1..7, parent.len())
        .expect("first relative view is within the parent coordinates");
    let parent_view = parent.view(parent_window);
    let ring_window = CoordWindow::checked(1..5, parent_view.len())
        .expect("second relative view is within the first visible window");
    let ring_coords = parent_view.view(ring_window);
    let x_address = ring_coords.xs().as_ptr().addr();
    let ring =
        Ring::try_from_closed(ring_coords).expect("fixture is a closed four-coordinate ring");
    (ring, x_address)
}
pub(super) fn rotated_ring(
    points: &[(f64, f64)],
    offset: usize,
    reverse: bool,
) -> crate::geometry::Ring {
    let mut vertices = points
        .iter()
        .take(points.len().saturating_sub(1))
        .copied()
        .collect::<Vec<_>>();
    vertices.rotate_left(offset);
    if reverse {
        vertices.reverse();
    }
    let first = vertices.first().copied().expect("ring has a vertex");
    vertices.push(first);
    ring_from_points(&vertices)
}
pub(super) fn admitted_ring_summary(
    frame: GeographicFrame,
    ring: &crate::geometry::Ring,
) -> RingSummary {
    let shape =
        crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), Vec::new()));
    let mut owners = GeographicCollectionOwners::default();
    let mut summary = None;
    ScalarGeographicView::try_new_with_collection_owners_and_visit(
        frame,
        &shape,
        &mut owners,
        |_| {},
        |event| {
            if let super::view::GeographicAdmissionEvent::Ring {
                summary: admitted, ..
            } = event
            {
                summary = Some(admitted);
            }
        },
    )
    .unwrap();
    summary.expect("an admitted polygon publishes its shell summary")
}
pub(super) fn nearby_grad_factor_frame() -> GeographicFrame {
    let factor = factor(std::f64::consts::TAU / 400.0_f64.next_up());
    GeographicFrame::try_new(factor, factor, 0.0).unwrap()
}
pub(super) fn axis_box(
    frame: GeographicFrame,
    west: f64,
    east: f64,
    south: f64,
    north: f64,
) -> RingSummary {
    let middle = f64::midpoint(west, east);
    let ring = ring(vec![west, middle, east, east, middle, west, west], vec![
        south, south, south, north, north, north, south,
    ]);
    RingSummary::try_new(frame, &GeographicRing::from_ring(&ring)).unwrap()
}
pub(super) fn deeply_nested_line(coordinate_count: usize) -> crate::geometry::Shape {
    use crate::geometry::Shape;

    let line = Shape::LineString(
        crate::geometry::LineSeq::try_new(xy_seq(
            (0..coordinate_count).map(|index| index as f64).collect(),
            vec![0.0; coordinate_count],
        ))
        .unwrap(),
    );
    (0..5).fold(line, |shape, _| {
        Shape::geometry_collection(vec![shape]).expect("test collection is valid")
    })
}
pub(super) fn exact_native_alias_oracle(longitude: f64, period: f64, periods: i128) -> f64 {
    use crate::geometry::numerics::product::exact_power_of_two;
    let (longitude_significand, longitude_exponent) = decompose(longitude.abs());
    let (period_significand, period_exponent) = decompose(period);
    let exponent = longitude_exponent.min(period_exponent);
    let aligned = |significand: u64, source_exponent: i32| {
        i128::from(significand)
            .checked_shl(u32::try_from(source_exponent.checked_sub(exponent).unwrap()).unwrap())
            .unwrap()
    };
    let longitude = aligned(longitude_significand, longitude_exponent)
        .checked_mul(if longitude.is_sign_negative() { -1 } else { 1 })
        .unwrap();
    let shift = aligned(period_significand, period_exponent)
        .checked_mul(periods)
        .unwrap();
    let sum = longitude.checked_add(shift).unwrap();
    sum as f64 * exact_power_of_two(exponent)
}
pub(super) fn exact_periodic_oracle(value: f64, period: f64) -> f64 {
    use crate::geometry::numerics::product::exact_power_of_two;
    let negative = value.is_sign_negative();
    let (significand, exponent) = decompose(value.abs());
    let (period_significand, period_exponent) = decompose(period);
    let shift = exponent
        .checked_sub(period_exponent)
        .expect("binary64 exponent difference fits i32");
    assert!(shift >= 0);
    let mut remainder = significand.rem_euclid(period_significand);
    for _ in 0..shift {
        let doubled = u128::from(remainder)
            .checked_mul(2)
            .expect("a reduced binary64 significand fits in u128");
        remainder = doubled.rem_euclid(u128::from(period_significand)) as u64;
    }
    let mut result = (remainder as f64) * exact_power_of_two(period_exponent);
    if negative {
        result = -result;
    }
    let half = period / 2.0;
    if result < -half {
        result += period;
    }
    if result >= half {
        result -= period;
    }
    if result == 0.0 { 0.0 } else { result }
}
pub(super) fn decompose(value: f64) -> (u64, i32) {
    let bits = value.to_bits();
    let biased = ((bits >> 52) & 0x7FF) as i32;
    assert!(biased > 0);
    (
        (bits & ((1_u64 << 52) - 1)) | (1_u64 << 52),
        biased
            .checked_sub(1023)
            .and_then(|exponent| exponent.checked_sub(52))
            .expect("normal binary64 exponent fits i32"),
    )
}
