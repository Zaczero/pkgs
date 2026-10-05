use super::*;
use crate::geographic::{AngularFactor, GeographicError, LongitudeWindows};
use crate::geometry::{CoordinateAxes, LineSeq, MultiMember, MultiPoint, Point, Ring, SharedSlice};

fn frame(radians_per_unit: f64) -> GeographicFrame {
    let unit = AngularFactor::try_new("test unit", radians_per_unit).unwrap();
    GeographicFrame::try_new(unit, unit, 0.0).unwrap()
}

fn native_frames() -> [GeographicFrame; 3] {
    [
        frame(std::f64::consts::PI / 180.0),
        frame(std::f64::consts::TAU / 400.0),
        frame(1.0),
    ]
}

fn line(xs: &[f64], ys: &[f64]) -> Shape {
    Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_vecs(xs.to_vec(), ys.to_vec(), None, None).unwrap())
            .unwrap(),
    )
}

fn multipoint(points: &[(f64, f64)]) -> Shape {
    Shape::MultiPoint(
        MultiPoint::try_new(
            CoordinateAxes::XY,
            points
                .iter()
                .map(|&(x, y)| MultiMember::Value(Point::new(x, y).unwrap()))
                .collect(),
        )
        .unwrap(),
    )
}

fn polygon(ring: &[(f64, f64)], holes: Vec<Ring>) -> Shape {
    Shape::Polygon(Polygon {
        shell: Ring::closed(
            ring.iter()
                .map(|&(x, y)| Point::new(x, y).unwrap())
                .collect(),
        )
        .unwrap(),
        holes: SharedSlice::from_vec(holes),
    })
}

fn cap_ring(frame: GeographicFrame, latitude: f64) -> Ring {
    let period = frame.longitude().period();
    Ring::closed(
        [0.0, period / 4.0, period / 2.0, period * 3.0 / 4.0]
            .into_iter()
            .map(|longitude| Point::new(longitude, latitude).unwrap())
            .collect(),
    )
    .unwrap()
}

fn cap_polygon(frame: GeographicFrame, latitude: f64) -> Shape {
    let ring = cap_ring(frame, latitude);
    let coordinates = ring
        .coords()
        .xs()
        .iter()
        .copied()
        .zip(ring.coords().ys().iter().copied())
        .collect::<Vec<_>>();
    polygon(&coordinates, Vec::new())
}

fn assert_close(actual: f64, expected: f64, period: f64) {
    assert!(
        (actual - expected).abs() <= period * 2.0e-14,
        "{actual} != {expected}"
    );
}

#[test]
fn p20_support_is_the_same_crossing_interval_for_admitted_alias_rows() {
    for frame in native_frames() {
        let axis = frame.longitude();
        let unit = axis.half_period() / 180.0;
        let period = axis.period();
        for shift in -2_i32..=2 {
            let offset = f64::from(shift) * period;
            let left = line(&[170.0 * unit + offset, 180.0 * unit + offset], &[0.0, 0.0]);
            let right = line(&[180.0 * unit + offset, 190.0 * unit + offset], &[
                0.0,
                10.0 * unit,
            ]);
            let envelope =
                GeographicSupportEnvelope::try_from_shapes(frame, &[&left, &right]).unwrap();
            let (west, east) = envelope.longitude.west_east(axis).unwrap();
            assert_close(west, 170.0 * unit, period);
            assert_close(east, -170.0 * unit, period);
            let (south, north) = envelope.latitude_bounds().unwrap();
            assert_close(south, 0.0, period);
            assert_close(north, 10.0 * unit, period);
        }
    }
}

#[test]
fn empty_admitted_shape_has_empty_periodic_support() {
    let frame = native_frames()[0];
    let empty = Shape::geometry_collection(Vec::new()).unwrap();
    let envelope = GeographicSupportEnvelope::try_from_shape(frame, &empty).unwrap();
    assert_eq!(envelope.longitude, CircularInterval::Empty);
    assert_eq!(envelope.latitude_bounds(), None);
}

#[test]
fn aggregate_uses_primitive_support_instead_of_expanded_row_hulls() {
    for frame in native_frames() {
        let axis = frame.longitude();
        let half = axis.half_period();
        let first_row = multipoint(&[
            (-half, 0.0),
            (-half / 2.0, 0.0),
            (0.0, 0.0),
            (half / 2.0, 0.0),
        ]);
        let second_row = multipoint(&[(half * 0.75, 0.0)]);
        let envelope =
            GeographicSupportEnvelope::try_from_shapes(frame, &[&first_row, &second_row]).unwrap();
        let CircularInterval::Arc { sweep, .. } = envelope.longitude else {
            panic!("quarter-turn point support has a bounded circular hull");
        };
        assert_close(sweep, axis.period() * 0.75, axis.period());

        let first = GeographicSupportEnvelope::try_from_shape(frame, &first_row).unwrap();
        let second = GeographicSupportEnvelope::try_from_shape(frame, &second_row).unwrap();
        let CircularInterval::Arc {
            sweep: expanded_sweep,
            ..
        } = union_circular_intervals(axis, &[first.longitude, second.longitude])
        else {
            panic!("expanding the row hull remains less than full longitude");
        };
        assert!(
            expanded_sweep > sweep + axis.period() / 16.0,
            "row hulls widened {sweep} to {expanded_sweep}"
        );
    }

    let frame = native_frames()[0];
    let first_row = multipoint(&[(0.0, 0.0), (120.0, 0.0), (240.0, 0.0)]);
    let second_row = multipoint(&[(180.0, 0.0)]);
    let envelope =
        GeographicSupportEnvelope::try_from_shapes(frame, &[&first_row, &second_row]).unwrap();
    assert_eq!(envelope.longitude, CircularInterval::Arc {
        start: frame.longitude().canonicalize(0.0).unwrap(),
        sweep: 240.0,
    });
    let first = GeographicSupportEnvelope::try_from_shape(frame, &first_row).unwrap();
    let second = GeographicSupportEnvelope::try_from_shape(frame, &second_row).unwrap();
    // The row-hull union leaves two 60-degree gaps. Its canonical-start tie
    // selects the seam at -180 degrees, producing the deliberately wider 300.
    assert_eq!(
        union_circular_intervals(frame.longitude(), &[first.longitude, second.longitude]),
        CircularInterval::Arc {
            start: frame.longitude().canonicalize(-180.0).unwrap(),
            sweep: 300.0,
        }
    );
}

#[test]
fn explicit_circle_edges_cover_full_longitude_even_at_the_same_quotient_point() {
    for frame in native_frames() {
        let period = frame.longitude().period();
        let circle = line(&[0.0, period], &[0.0, 0.0]);
        let envelope = GeographicSupportEnvelope::try_from_shape(frame, &circle).unwrap();
        assert_eq!(envelope.longitude, CircularInterval::Full);
    }
}

#[test]
fn half_turn_edges_retain_their_selected_closed_arc() {
    for frame in native_frames() {
        let axis = frame.longitude();
        let half = axis.half_period();
        let eastward = line(&[0.0, half], &[0.0, 0.0]);
        let westward = line(&[0.0, -half], &[0.0, 0.0]);
        let reversed_eastward = line(&[half, 0.0], &[0.0, 0.0]);

        let eastward_envelope =
            GeographicSupportEnvelope::try_from_shape(frame, &eastward).unwrap();
        let westward_envelope =
            GeographicSupportEnvelope::try_from_shape(frame, &westward).unwrap();
        let reversed_envelope =
            GeographicSupportEnvelope::try_from_shape(frame, &reversed_eastward).unwrap();
        assert_eq!(
            eastward_envelope.longitude,
            CircularInterval::try_arc(axis, 0.0, half).unwrap()
        );
        assert_eq!(
            westward_envelope.longitude,
            CircularInterval::try_arc(axis, -half, half).unwrap()
        );
        assert_eq!(reversed_envelope.longitude, eastward_envelope.longitude);
    }
}

#[test]
fn full_longitude_bands_are_owned_by_ring_domain() {
    for frame in native_frames() {
        let axis = frame.longitude();
        let half = axis.half_period();
        let pole = frame.pole();
        let shape = polygon(
            &[
                (-half, -pole / 2.0),
                (-half / 2.0, -pole / 2.0),
                (0.0, -pole / 2.0),
                (half / 2.0, -pole / 2.0),
                (half, -pole / 2.0),
                (half, pole / 2.0),
                (half / 2.0, pole / 2.0),
                (0.0, pole / 2.0),
                (-half / 2.0, pole / 2.0),
                (-half, pole / 2.0),
            ],
            Vec::new(),
        );
        let envelope = GeographicSupportEnvelope::try_from_shape(frame, &shape).unwrap();
        assert_eq!(envelope.longitude, CircularInterval::Full);
        assert_eq!(envelope.latitude_bounds(), Some((-pole / 2.0, pole / 2.0)));
    }
}

#[test]
fn a_cap_hole_removes_its_pole_from_occupied_latitude_extrema() {
    for frame in native_frames() {
        let pole = frame.pole();
        let shell = cap_ring(frame, pole * 0.5);
        let hole = cap_ring(frame, pole * 0.75);
        let shape = polygon(
            &shell
                .coords()
                .xs()
                .iter()
                .copied()
                .zip(shell.coords().ys().iter().copied())
                .collect::<Vec<_>>(),
            vec![hole],
        );
        let envelope = GeographicSupportEnvelope::try_from_shape(frame, &shape).unwrap();
        assert_eq!(envelope.longitude, CircularInterval::Full);
        let (_, north) = envelope.latitude_bounds().unwrap();
        assert_eq!(north, pole * 0.75);
    }
}

#[test]
fn a_union_member_can_reoccupy_a_pole_removed_by_another_polygon_hole() {
    for frame in native_frames() {
        let shell = cap_ring(frame, frame.pole() / 2.0);
        let shell_coordinates = shell
            .coords()
            .xs()
            .iter()
            .copied()
            .zip(shell.coords().ys().iter().copied())
            .collect::<Vec<_>>();
        let annulus = polygon(&shell_coordinates, vec![cap_ring(
            frame,
            frame.pole() * 0.75,
        )]);
        let cap = cap_polygon(frame, frame.pole() / 2.0);
        let members = Shape::geometry_collection(vec![annulus, cap]).unwrap();
        let envelope = GeographicSupportEnvelope::try_from_shape(frame, &members).unwrap();
        assert_eq!(envelope.longitude, CircularInterval::Full);
        assert_eq!(
            envelope.latitude_bounds(),
            Some((frame.pole() / 2.0, frame.pole()))
        );
    }
}

#[test]
fn pole_points_and_wedges_do_not_imply_full_longitude() {
    for frame in native_frames() {
        let axis = frame.longitude();
        for sign in [-1.0, 1.0] {
            let pole = sign * frame.pole();
            let longitude = axis.half_period() / 3.0;
            let point = Shape::Point(Point::new(longitude, pole).unwrap());
            let point_envelope = GeographicSupportEnvelope::try_from_shape(frame, &point).unwrap();
            assert_eq!(point_envelope.longitude, CircularInterval::Arc {
                start: axis.canonicalize(longitude).unwrap(),
                sweep: 0.0,
            });
            assert_eq!(point_envelope.latitude_bounds(), Some((pole, pole)));

            let meridian = line(&[longitude, longitude], &[sign * frame.pole() * 0.8, pole]);
            let meridian_envelope =
                GeographicSupportEnvelope::try_from_shape(frame, &meridian).unwrap();
            assert_eq!(meridian_envelope.longitude, CircularInterval::Arc {
                start: axis.canonicalize(longitude).unwrap(),
                sweep: 0.0,
            });
            assert_eq!(
                meridian_envelope.latitude_bounds(),
                Some((pole.min(pole * 0.8), pole.max(pole * 0.8)))
            );

            let wedge = polygon(
                &[
                    (0.0, sign * frame.pole() * 0.8),
                    (axis.half_period() / 3.0, pole),
                    (axis.half_period() * 2.0 / 3.0, sign * frame.pole() * 0.8),
                ],
                Vec::new(),
            );
            let wedge_envelope = GeographicSupportEnvelope::try_from_shape(frame, &wedge).unwrap();
            assert_ne!(wedge_envelope.longitude, CircularInterval::Full);
            assert_eq!(
                wedge_envelope.longitude.west_east(axis),
                Some((0.0, axis.half_period() * 2.0 / 3.0))
            );
        }
    }
}

#[test]
fn mixed_orientation_seam_edges_merge_to_the_smallest_cover() {
    for frame in native_frames() {
        let unit = frame.longitude().half_period() / 180.0;
        let eastward = line(&[170.0 * unit, -170.0 * unit], &[0.0, 1.0]);
        let westward = line(&[-175.0 * unit, 175.0 * unit], &[2.0 * unit, 3.0 * unit]);
        let envelope =
            GeographicSupportEnvelope::try_from_shapes(frame, &[&eastward, &westward]).unwrap();
        let (west, east) = envelope.longitude.west_east(frame.longitude()).unwrap();
        assert_close(west, 170.0 * unit, frame.longitude().period());
        assert_close(east, -170.0 * unit, frame.longitude().period());
        assert!(matches!(
            envelope.longitude.windows(frame.longitude()),
            Some(LongitudeWindows::Two(..))
        ));
    }
}

#[test]
fn ordinary_linear_fast_path_keeps_its_admitted_extrema() {
    let frame = native_frames()[0];
    let shape = line(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]);
    let envelope = GeographicSupportEnvelope::try_from_shape(frame, &shape).unwrap();

    let CircularInterval::Arc { start, sweep } = envelope.longitude else {
        panic!("a strict subhalf line has a bounded longitude arc");
    };
    assert_eq!(start.get(), 1.0);
    assert_eq!(sweep, 2.0);
    assert_eq!(envelope.latitude_bounds(), Some((4.0, 6.0)));
}

#[test]
fn ordinary_linear_fast_path_does_not_allocate() {
    let frame = native_frames()[0];
    let shape = line(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]);
    let (envelope, allocations) = crate::test_support::allocation_count(|| {
        GeographicSupportEnvelope::try_from_shape(frame, &shape)
    });
    envelope.expect("ordinary geographic support succeeds");
    assert_eq!(allocations, 0);
}

#[test]
fn scalar_support_bounds_do_not_allocate_adapter_storage() {
    let frame = native_frames()[0];
    let shape = line(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]);
    let (bounds, allocations) = crate::test_support::allocation_count(|| {
        crate::geometry::geographic_crossing_bounds_in_frame(&shape, frame)
    });
    assert_eq!(
        bounds,
        Some(crate::geometry::Bounds::new_raw(1.0, 4.0, 3.0, 6.0))
    );
    assert_eq!(allocations, 0);
}

#[test]
fn aggregate_support_bounds_use_at_most_one_handle_allocation() {
    let frame = native_frames()[0];
    let shapes = [
        line(&[1.0, 2.0], &[4.0, 5.0]),
        line(&[3.0, 4.0], &[6.0, 7.0]),
    ];
    let (bounds, allocations) = crate::test_support::allocation_count(|| {
        crate::geometry::geographic_crossing_bounds_for_shapes_in_frame(shapes.iter(), frame)
    });
    assert_eq!(
        bounds,
        Some(crate::geometry::Bounds::new_raw(1.0, 4.0, 4.0, 7.0))
    );
    assert!(
        allocations <= 1,
        "{allocations} allocations for aggregate bounds"
    );
}

#[test]
fn scalar_and_owned_aggregate_bounds_keep_seam_band_pole_and_shared_support() {
    let frame = native_frames()[0];
    let seam = line(&[170.0, -170.0], &[0.0, 10.0]);
    let shared = Shape::geometry_collection(vec![seam.clone(), seam.clone()]).unwrap();
    let band = polygon(
        &[
            (-180.0, -45.0),
            (-90.0, -45.0),
            (0.0, -45.0),
            (90.0, -45.0),
            (180.0, -45.0),
            (180.0, 45.0),
            (90.0, 45.0),
            (0.0, 45.0),
            (-90.0, 45.0),
            (-180.0, 45.0),
        ],
        Vec::new(),
    );
    for (name, shape, expected) in [
        (
            "seam",
            seam,
            crate::geometry::Bounds::new_raw(170.0, 0.0, -170.0, 10.0),
        ),
        (
            "band",
            band,
            crate::geometry::Bounds::new_raw(-180.0, -45.0, 180.0, 45.0),
        ),
        (
            "pole",
            cap_polygon(frame, 45.0),
            crate::geometry::Bounds::new_raw(-180.0, 45.0, 180.0, 90.0),
        ),
        (
            "shared",
            shared,
            crate::geometry::Bounds::new_raw(170.0, 0.0, -170.0, 10.0),
        ),
    ] {
        assert_eq!(
            crate::geometry::geographic_crossing_bounds_in_frame(&shape, frame),
            Some(expected),
            "{name} scalar bounds"
        );
        assert_eq!(
            crate::geometry::geographic_crossing_bounds_for_shapes_in_frame([shape], frame),
            Some(expected),
            "{name} owned aggregate bounds"
        );
    }
}

#[test]
fn geographic_support_admission_preserves_invalid_coordinate_diagnostics() {
    let frame = native_frames()[0];
    let shape = Shape::Point(Point::new_raw_xy(f64::INFINITY, 100.0));
    let Err(GeographicAdmissionError::Geographic(error)) =
        GeographicSupportEnvelope::try_from_shape(frame, &shape)
    else {
        panic!("invalid coordinates retain their geographic admission diagnostic");
    };
    assert!(matches!(
        error.source,
        GeographicError::NonFinite {
            name: "longitude",
            value
        } if value == f64::INFINITY
    ));
}

#[test]
fn shared_collection_support_and_census_remain_linear() {
    let frame = native_frames()[0];
    let base = line(&[170.0, -170.0], &[0.0, 1.0]);
    let expected = GeographicSupportEnvelope::try_from_shape(frame, &base).unwrap();
    for depth in [16_usize, 128] {
        let mut shape = base.clone();
        for _ in 0..depth {
            shape = Shape::geometry_collection(vec![shape.clone(), shape])
                .expect("nested shared collection stays within the depth limit");
        }

        let memo = GeographicSupportOwnerMemo::new(&shape).unwrap();
        assert_eq!(memo.census_work.collection_descents, depth);
        assert_eq!(
            memo.census_work.collection_occurrences,
            depth.wrapping_mul(2).wrapping_sub(1)
        );
        assert_eq!(memo.census_work.repeated_owner_occurrences, depth);
        assert_eq!(memo.census_work.collection_member_slots, depth * 2);
        assert_eq!(memo.census_work.coordinate_run_visits, 2);
        assert_eq!(memo.census_work.owner_map_initializations, 1);

        let actual = GeographicSupportEnvelope::try_from_shape(frame, &shape).unwrap();
        assert_eq!(actual, expected);
        let work = geographic_support_work().expect("support work was recorded");
        assert_eq!(
            work.admission.collection_occurrences,
            depth.wrapping_mul(2).wrapping_sub(1)
        );
        assert_eq!(work.admission.collection_descents, depth);
        assert_eq!(work.admission.collection_member_slots, depth * 2);
        assert_eq!(work.admission.coordinate_visits, 2);
        assert_eq!(
            work.admission.repeated_owner_occurrences,
            depth.wrapping_sub(1)
        );
        assert_eq!(work.census.collection_descents, depth);
        assert_eq!(work.census.collection_member_slots, depth * 2);
        assert_eq!(work.census.coordinate_run_visits, 2);
        assert_eq!(work.census.repeated_owner_occurrences, depth);
    }
}

#[test]
fn distinct_support_owners_do_not_allocate_a_census_map() {
    let lines = (0..256)
        .map(|index| {
            let x = f64::from(index);
            line(&[x, x + 0.25], &[0.0, 0.5])
        })
        .collect();
    let shape = Shape::geometry_collection(lines).unwrap();
    let memo = GeographicSupportOwnerMemo::new(&shape).unwrap();
    assert!(memo.owners.is_none());
    assert_eq!(memo.census_work.collection_occurrences, 1);
    assert_eq!(memo.census_work.collection_descents, 1);
    assert_eq!(memo.census_work.collection_member_slots, 256);
    assert_eq!(memo.census_work.coordinate_run_visits, 256);
    assert_eq!(memo.census_work.repeated_owner_occurrences, 0);
    assert_eq!(memo.census_work.owner_map_initializations, 0);
}

#[test]
fn repeated_multipoint_coordinate_owners_contribute_support_once() {
    let frame = native_frames()[0];
    let base = multipoint(&[(170.0, 0.0), (-170.0, 10.0)]);
    let expected = GeographicSupportEnvelope::try_from_shape(frame, &base).unwrap();
    let repeated = Shape::geometry_collection(vec![base; 256]).unwrap();
    let actual = GeographicSupportEnvelope::try_from_shape(frame, &repeated).unwrap();
    assert_eq!(actual, expected);
    let work = geographic_support_work().expect("support work was recorded");
    assert_eq!(work.census.repeated_owner_occurrences, 255);
    assert_eq!(work.census.owner_map_initializations, 1);
}

#[test]
fn repeated_coordinate_runs_across_shapes_contribute_support_once() {
    let frame = native_frames()[0];
    let base = multipoint(&[(170.0, 0.0), (-170.0, 10.0)]);
    let first_row = base.clone();
    let second_row = base.clone();
    let expected = GeographicSupportEnvelope::try_from_shape(frame, &base).unwrap();
    let actual =
        GeographicSupportEnvelope::try_from_shapes(frame, &[first_row, second_row]).unwrap();

    assert_eq!(actual, expected);
    let work = geographic_support_work().expect("support work was recorded");
    assert_eq!(work.census.coordinate_run_visits, 2);
    assert_eq!(work.census.repeated_owner_occurrences, 1);
    assert_eq!(work.census.owner_map_initializations, 1);
}

#[test]
fn a_cap_collection_member_preserves_occupied_pole_extrema() {
    let frame = native_frames()[0];
    let shell = cap_ring(frame, frame.pole() / 2.0);
    let ring = shell
        .coords()
        .xs()
        .iter()
        .copied()
        .zip(shell.coords().ys().iter().copied())
        .collect::<Vec<_>>();
    let cap = polygon(&ring, Vec::new());
    let expected = GeographicSupportEnvelope::try_from_shape(frame, &cap).unwrap();
    let repeated = Shape::geometry_collection(vec![cap]).unwrap();
    let actual = GeographicSupportEnvelope::try_from_shape(frame, &repeated).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        actual.latitude_bounds(),
        Some((frame.pole() / 2.0, frame.pole()))
    );
}

#[test]
fn shared_collection_ring_memo_replays_occupied_pole_effects() {
    let frame = native_frames()[0];
    let cap = cap_polygon(frame, frame.pole() / 2.0);
    let shared = Shape::geometry_collection(vec![cap]).unwrap();
    let middle = Shape::Point(Point::new(10.0, 0.0).unwrap());
    let repeated =
        Shape::geometry_collection(vec![shared.clone(), middle.clone(), shared]).unwrap();
    let expanded = Shape::geometry_collection(vec![
        Shape::geometry_collection(vec![cap_polygon(frame, frame.pole() / 2.0)]).unwrap(),
        middle,
        Shape::geometry_collection(vec![cap_polygon(frame, frame.pole() / 2.0)]).unwrap(),
    ])
    .unwrap();

    let repeated = GeographicSupportEnvelope::try_from_shape(frame, &repeated).unwrap();
    let expanded = GeographicSupportEnvelope::try_from_shape(frame, &expanded).unwrap();
    assert_eq!(repeated, expanded);
    assert_eq!(repeated.latitude_bounds(), Some((0.0, frame.pole())));
    assert_eq!(repeated.longitude, CircularInterval::Full);
}
