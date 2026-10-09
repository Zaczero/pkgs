#[test]
fn authenticated_rectangle_moments_match_exact_source_fans() {
    use crate::geographic::{AngularFactor, GeographicAxisOrientation};
    use crate::geometry::Point;

    for (longitude_east, latitude_north) in
        [(true, true), (false, true), (true, false), (false, false)]
    {
        let unit = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
        let frame = GeographicFrame::try_new_with_orientation(
            unit,
            unit,
            0.0,
            GeographicAxisOrientation::from_axis_directions(longitude_east, latitude_north),
        )
        .unwrap();

        for points in [
            [(13.0, 52.0), (14.0, 52.0), (14.0, 53.0), (13.0, 53.0)],
            [(179.8, -1.0), (-179.8, -1.0), (-179.8, 1.0), (179.8, 1.0)],
            [(0.0, 10.0), (360.0, 10.0), (360.0, 20.0), (0.0, 20.0)],
            [
                (0.0, 0.0),
                (1.0e-160, 0.0),
                (1.0e-160, 1.0e-160),
                (0.0, 1.0e-160),
            ],
            [(0.0, 0.0), (0.0, 4.0), (4.0, 4.0), (4.0, 0.0)],
        ] {
            let ring = Ring::closed(
                points
                    .iter()
                    .map(|&(x, y)| Point::new_raw_xy(x, y))
                    .collect(),
            )
            .unwrap();
            let longitude_scale = 1.0 / 64.0;
            let x_scale = Bounded::exact(
                frame.longitude().radians_per_unit() * frame.longitude_orientation_sign(),
            )
            .mul(Bounded::exact(longitude_scale));
            let y_scale = Bounded::exact(
                frame.latitude_radians_per_unit() * frame.latitude_orientation_sign(),
            )
            .mul(Bounded::exact(LATITUDE_SCALE));
            let mut moments: Vec<BoundedSum> = std::iter::repeat_with(BoundedSum::default)
                .take(13)
                .collect();
            let mut variation = 0.0;
            let mut maximum_latitude = 0.0;
            assert!(rectangle_moments(
                &ring,
                frame,
                0.0,
                x_scale,
                y_scale,
                &mut moments,
                &mut variation,
                &mut maximum_latitude,
            ));

            let mut exact = vec![ExactDyadic::zero(); 13];
            let mut bounds = ExactSourceBounds::default();
            let first = ring.nth_coord(0);
            let first_connection = frame
                .longitude()
                .chart_connection(first.x, ring.nth_coord(1).x)
                .unwrap();
            let origin = first_connection.start.get();
            let mut previous = first;
            let mut turn = 0_i64;
            let mut x = exact_scaled_x(frame, origin, 0, origin, longitude_scale).unwrap();
            let mut z = exact_scaled_y(frame, first.y, 0.0).unwrap();
            for index in 1..ring.coord_count() {
                let next = ring.nth_coord(index);
                let edge = frame
                    .longitude()
                    .chart_connection(previous.x, next.x)
                    .unwrap();
                turn += i64::from(edge.source_lift);
                let next_x =
                    exact_scaled_x(frame, edge.end.get(), turn, origin, longitude_scale).unwrap();
                let next_z = exact_scaled_y(frame, next.y, 0.0).unwrap();
                add_exact_edge(&x, &z, &next_x, &next_z, &mut exact, &mut bounds).unwrap();
                previous = next;
                x = next_x;
                z = next_z;
            }
            assert_eq!(turn, 0);
            for (degree, (moment, exact)) in moments.iter().zip(&exact).enumerate() {
                let residual = exact
                    .clone()
                    .try_subtract(ExactDyadic::from_f64(moment.value()))
                    .unwrap()
                    .abs();
                assert!(
                    residual.cmp(&ExactDyadic::from_f64(moment.error())).is_le(),
                    "degree={degree}, axes=({longitude_east}, {latitude_north}), points={points:?}"
                );
            }
        }
    }
}

#[test]
fn authenticated_rectangle_declines_a_half_period_edge() {
    use crate::geographic::GeographicFrame;
    use crate::geometry::Point;

    let frame = GeographicFrame::crs84();
    let ring = Ring::closed(
        [(0.0, 0.0), (180.0, 0.0), (180.0, 4.0), (0.0, 4.0)]
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect(),
    )
    .unwrap();
    let mut moments: Vec<BoundedSum> = std::iter::repeat_with(BoundedSum::default)
        .take(5)
        .collect();
    let mut variation = 0.0;
    let mut maximum_latitude = 0.0;
    let longitude_scale = Bounded::exact(1.0 / 64.0);
    let latitude_scale = Bounded::exact(LATITUDE_SCALE);

    assert!(!rectangle_moments(
        &ring,
        frame,
        0.0,
        longitude_scale,
        latitude_scale,
        &mut moments,
        &mut variation,
        &mut maximum_latitude,
    ));
    assert!(moments.iter().all(|moment| moment.value() == 0.0));
    assert_eq!(variation, 0.0);
    assert_eq!(maximum_latitude, 0.0);
}
