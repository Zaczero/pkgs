use super::*;
use crate::geographic::{AngularFactor, GeographicError, LongitudeWindows};
use crate::geometry::{CoordinateAxes, MultiMember, MultiPoint, SharedSlice};

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

fn arc_width(period: f64, start: f64, end: f64) -> f64 {
    if start <= end {
        end - start
    } else {
        period - start + end
    }
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
            let (west, east) = envelope.longitude.west_east().unwrap();
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
        let CircularInterval::Arc { start, end } = envelope.longitude else {
            panic!("quarter-turn point support has a bounded circular hull");
        };
        assert_close(start.get(), -half / 2.0, axis.period());
        assert_close(end.get(), -half, axis.period());
        let sweep = arc_width(axis.period(), start.get(), end.get());
        assert_close(sweep, axis.period() * 0.75, axis.period());

        let first = GeographicSupportEnvelope::try_from_shape(frame, &first_row).unwrap();
        let second = GeographicSupportEnvelope::try_from_shape(frame, &second_row).unwrap();
        let CircularInterval::Arc {
            start: expanded_start,
            end: expanded_end,
        } = union_circular_intervals(axis, &[first.longitude, second.longitude])
        else {
            panic!("expanding the row hull remains less than full longitude");
        };
        let expanded_sweep = arc_width(axis.period(), expanded_start.get(), expanded_end.get());
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
    assert_eq!(
        envelope.longitude,
        CircularInterval::from_endpoints(
            frame.longitude().canonicalize(0.0).unwrap(),
            frame.longitude().canonicalize(-120.0).unwrap(),
        )
    );
    let first = GeographicSupportEnvelope::try_from_shape(frame, &first_row).unwrap();
    let second = GeographicSupportEnvelope::try_from_shape(frame, &second_row).unwrap();
    // The row-hull union leaves two 60-degree gaps. Its canonical-start tie
    // selects the seam at -180 degrees, producing the deliberately wider 300.
    assert_eq!(
        union_circular_intervals(frame.longitude(), &[first.longitude, second.longitude]),
        CircularInterval::from_endpoints(
            frame.longitude().canonicalize(-180.0).unwrap(),
            frame.longitude().canonicalize(120.0).unwrap(),
        )
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
            CircularInterval::from_endpoints(
                axis.canonicalize(0.0).unwrap(),
                axis.canonicalize(half).unwrap()
            )
        );
        assert_eq!(
            westward_envelope.longitude,
            CircularInterval::from_endpoints(
                axis.canonicalize(-half).unwrap(),
                axis.canonicalize(0.0).unwrap()
            )
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
            assert_eq!(
                point_envelope.longitude,
                CircularInterval::point(axis.canonicalize(longitude).unwrap())
            );
            assert_eq!(point_envelope.latitude_bounds(), Some((pole, pole)));

            let meridian = line(&[longitude, longitude], &[sign * frame.pole() * 0.8, pole]);
            let meridian_envelope =
                GeographicSupportEnvelope::try_from_shape(frame, &meridian).unwrap();
            assert_eq!(
                meridian_envelope.longitude,
                CircularInterval::point(axis.canonicalize(longitude).unwrap())
            );
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
                wedge_envelope.longitude.west_east(),
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
        let (west, east) = envelope.longitude.west_east().unwrap();
        assert_close(west, 170.0 * unit, frame.longitude().period());
        assert_close(east, -170.0 * unit, frame.longitude().period());
        assert!(matches!(
            envelope.longitude.windows(frame.longitude()),
            Some(LongitudeWindows::Two(..))
        ));
    }
}

#[test]
fn support_products_keep_periodic_support_separate_from_raw_extrema() {
    let frame = native_frames()[0];
    let crossing = line(&[170.0, -170.0], &[0.0, 10.0]);
    let products = GeographicSupportProducts::try_from_shape(frame, &crossing).unwrap();

    assert_eq!(
        products.raw_extrema,
        Some(crate::geometry::Bounds::new_raw(-170.0, 0.0, 170.0, 10.0))
    );
    assert_eq!(
        products.support.bounds(frame),
        Some(crate::geometry::Bounds::new_raw(170.0, 0.0, -170.0, 10.0))
    );
    assert_eq!(
        products.public_bounds(frame),
        Some(crate::geometry::Bounds::new_raw(170.0, 0.0, -170.0, 10.0))
    );
    assert_eq!(products.work, GeographicSupportWork {
        coordinate_visits: 2,
        point_events: 0,
        point_intervals: 0,
        segment_connections: 1,
        segment_intervals: 1,
        ring_summaries: 0,
    });

    let point_cloud = multipoint(&[(0.0, 0.0), (120.0, 0.0), (-120.0, 0.0), (180.0, 0.0)]);
    let products = GeographicSupportProducts::try_from_shape(frame, &point_cloud).unwrap();
    assert_eq!(
        products.raw_extrema,
        Some(crate::geometry::Bounds::new_raw(-120.0, 0.0, 180.0, 0.0))
    );
    let support_bounds = products.support.bounds(frame).unwrap();
    assert_eq!(products.public_bounds(frame), Some(support_bounds));
    assert_ne!(products.public_bounds(frame), products.raw_extrema);
    let (west, east) = products.support.longitude.west_east().unwrap();
    let width = if west <= east {
        east - west
    } else {
        frame.longitude().period() - (west - east)
    };
    assert_close(width, 240.0, frame.longitude().period());

    let same_seam = line(&[180.0, 180.0], &[0.0, 10.0]);
    assert_eq!(
        GeographicSupportProducts::try_from_shape(frame, &same_seam)
            .unwrap()
            .public_bounds(frame),
        Some(crate::geometry::Bounds::new_raw(-180.0, 0.0, -180.0, 10.0))
    );

    let opposite_seam = line(&[-180.0, 180.0], &[0.0, 10.0]);
    assert_eq!(
        GeographicSupportProducts::try_from_shape(frame, &opposite_seam)
            .unwrap()
            .public_bounds(frame),
        Some(crate::geometry::Bounds::new_raw(-180.0, 0.0, -180.0, 10.0))
    );
}

#[test]
fn ordinary_linear_fast_path_keeps_its_admitted_extrema() {
    let frame = native_frames()[0];
    let shape = line(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]);
    let envelope = GeographicSupportEnvelope::try_from_shape(frame, &shape).unwrap();

    let CircularInterval::Arc { start, end } = envelope.longitude else {
        panic!("a strict subhalf line has a bounded longitude arc");
    };
    assert_eq!(start.get(), 1.0);
    assert_eq!(end.get(), 3.0);
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
fn public_ordinary_bounds_keep_the_raw_extrema_fast_path_allocation_free() {
    let frame = native_frames()[0];
    let shape = line(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]);
    let raw = shape.bounds();
    let (bounds, allocations) = crate::test_support::allocation_count(|| {
        GeographicSupportProducts::public_bounds_for_shape(frame, &shape, raw)
    });
    assert_eq!(bounds, raw);
    assert_eq!(allocations, 0);
}

#[test]
fn ordinary_support_realization_preserves_the_represented_extrema() {
    let frame = native_frames()[0];
    let raw = crate::geometry::Bounds::new_raw(-0.1, 1.0, 0.2, 2.0);
    let support = GeographicSupportEnvelope::from_ordinary_bounds(frame, raw).unwrap();
    let realized = support.into_shape(frame).unwrap();
    assert_eq!(realized.bounds(), Some(raw));
}

#[test]
fn wrapped_support_realization_copies_endpoints_and_uses_only_canonical_seam_vertices() {
    let frame = native_frames()[0];
    let shape = line(&[170.0, -170.0], &[-10.0, 10.0]);
    let support = GeographicSupportEnvelope::try_from_shape(frame, &shape).unwrap();
    let Shape::Polygon(polygon) = support.into_shape(frame).unwrap() else {
        panic!("a wrapped two-dimensional support realizes as a polygon");
    };
    let ring = polygon
        .rings()
        .next()
        .expect("polygon has an exterior ring");
    assert_eq!(ring.coords().xs(), &[
        170.0, -180.0, -170.0, -170.0, -180.0, 170.0, 170.0
    ]);
    assert!(
        ring.coords()
            .xs()
            .iter()
            .all(|&longitude| longitude.to_bits() != 180.0_f64.to_bits()),
        "the positive seam sentinel is not a computed vertex"
    );
}

#[test]
fn full_support_realization_uses_quarter_turns_and_repeats_the_first_record() {
    let frame = native_frames()[0];
    let period = frame.longitude().period();
    let shape = line(&[0.0, period], &[-10.0, 10.0]);
    let support = GeographicSupportEnvelope::try_from_shape(frame, &shape).unwrap();
    assert_eq!(support.longitude, CircularInterval::Full);
    let Shape::Polygon(polygon) = support.into_shape(frame).unwrap() else {
        panic!("a full-longitude band realizes as a polygon");
    };
    let ring = polygon
        .rings()
        .next()
        .expect("polygon has an exterior ring");
    assert_eq!(ring.coords().xs(), &[
        -180.0, -90.0, 0.0, 90.0, -180.0, -180.0, 90.0, 0.0, -90.0, -180.0, -180.0,
    ]);
    assert_eq!(
        ring.coords().ys().first(),
        ring.coords().ys().last(),
        "ring closure repeats its first latitude"
    );
    assert!(
        ring.coords()
            .xs()
            .iter()
            .all(|&longitude| longitude.to_bits() != 180.0_f64.to_bits()),
        "the positive seam sentinel is not a computed vertex"
    );
}

#[test]
fn scalar_support_bounds_do_not_allocate_adapter_storage() {
    let frame = native_frames()[0];
    let shape = line(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]);
    let (bounds, allocations) = crate::test_support::allocation_count(|| {
        GeographicSupportEnvelope::try_from_shape(frame, &shape)
            .ok()
            .and_then(|support| support.bounds(frame))
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
        GeographicSupportEnvelope::try_from_shapes(frame, &shapes)
            .ok()
            .and_then(|support| support.bounds(frame))
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
            GeographicSupportEnvelope::try_from_shape(frame, &shape)
                .ok()
                .and_then(|support| support.bounds(frame)),
            Some(expected),
            "{name} scalar bounds"
        );
        assert_eq!(
            GeographicSupportEnvelope::try_from_shapes(frame, &[shape])
                .ok()
                .and_then(|support| support.bounds(frame)),
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
fn shared_collection_support_preserves_its_periodic_hull() {
    let frame = native_frames()[0];
    let base = line(&[170.0, -170.0], &[0.0, 1.0]);
    let expected = GeographicSupportEnvelope::try_from_shape(frame, &base).unwrap();
    for depth in [16_usize, 128] {
        let mut shape = base.clone();
        for _ in 0..depth {
            shape = Shape::geometry_collection(vec![shape.clone(), shape])
                .expect("nested shared collection stays within the depth limit");
        }

        let products = GeographicSupportProducts::try_from_shape(frame, &shape).unwrap();
        assert_eq!(products.support, expected);
        assert_eq!(products.work, GeographicSupportWork {
            coordinate_visits: 2,
            point_events: 0,
            point_intervals: 0,
            segment_connections: 1,
            segment_intervals: 1,
            ring_summaries: 0,
        });
    }
}

#[test]
fn ring_support_reuses_admitted_edge_connections() {
    let frame = native_frames()[0];
    let shell = cap_ring(frame, frame.pole() / 2.0);
    let coordinate_count = shell.coords().len();
    let points = shell
        .coords()
        .xs()
        .iter()
        .copied()
        .zip(shell.coords().ys().iter().copied())
        .collect::<Vec<_>>();
    let shape = polygon(&points, Vec::new());
    let products = GeographicSupportProducts::try_from_shape(frame, &shape).unwrap();

    assert_eq!(products.work.coordinate_visits, coordinate_count);
    assert_eq!(
        products.work.segment_connections,
        coordinate_count.saturating_sub(1)
    );
    assert_eq!(products.work.segment_intervals, 1);
    assert_eq!(products.work.ring_summaries, 1);
}

#[test]
fn repeated_multipoint_coordinate_owners_preserve_support() {
    let frame = native_frames()[0];
    let base = multipoint(&[(170.0, 0.0), (-170.0, 10.0)]);
    let expected = GeographicSupportEnvelope::try_from_shape(frame, &base).unwrap();
    let repeated = Shape::geometry_collection(vec![base; 256]).unwrap();
    let products = GeographicSupportProducts::try_from_shape(frame, &repeated).unwrap();
    assert_eq!(products.support, expected);
    assert_eq!(products.work.coordinate_visits, 2);
    assert_eq!(products.work.point_events, 2);
    assert_eq!(products.work.point_intervals, 2);
}

#[test]
fn repeated_coordinate_runs_across_shapes_preserve_support() {
    let frame = native_frames()[0];
    let base = multipoint(&[(170.0, 0.0), (-170.0, 10.0)]);
    let first_row = base.clone();
    let second_row = base.clone();
    let expected = GeographicSupportEnvelope::try_from_shape(frame, &base).unwrap();
    let products =
        GeographicSupportProducts::try_from_shapes(frame, &[first_row, second_row]).unwrap();

    assert_eq!(products.support, expected);
    assert_eq!(products.work.coordinate_visits, 4);
    assert_eq!(products.work.point_events, 4);
    assert_eq!(products.work.point_intervals, 2);
}

#[test]
fn connected_run_components_preserve_native_endpoints_and_frame_variants() {
    for (factor, east, north, prime) in [
        (std::f64::consts::PI / 180.0, true, true, 0.0),
        (std::f64::consts::TAU / 400.0, true, true, 0.0),
        (1.0, true, true, 0.0),
        (std::f64::consts::PI / 180.0, false, false, 0.25),
        (std::f64::consts::TAU / 400.0, false, true, 0.31),
        (1.0, true, false, -0.2),
    ] {
        let unit = AngularFactor::try_new("native component test", factor).unwrap();
        let frame = GeographicFrame::try_new_with_orientation(
            unit,
            unit,
            prime,
            super::super::GeographicAxisOrientation::from_axis_directions(east, north),
        )
        .unwrap();
        let period = frame.longitude().period();
        let half = period / 2.0;
        let tiny = -f64::from_bits(1);
        for (xs, expected) in [
            (vec![0.0, period], Bounds::new_raw(-half, 0.0, half, 0.0)),
            (vec![0.0, -period], Bounds::new_raw(-half, 0.0, half, 0.0)),
            (vec![0.0, half], Bounds::new_raw(0.0, 0.0, -half, 0.0)),
            (vec![0.0, -half], Bounds::new_raw(-half, 0.0, 0.0, 0.0)),
            (vec![half, 0.0], Bounds::new_raw(0.0, 0.0, -half, 0.0)),
            (
                vec![0.0, period / 3.0, 2.0 * period / 3.0, tiny],
                Bounds::new_raw(0.0, 0.0, tiny, 0.0),
            ),
            (
                vec![half.next_down(), -half],
                Bounds::new_raw(half.next_down(), 0.0, -half, 0.0),
            ),
        ] {
            let ys = vec![0.0; xs.len()];
            let products =
                GeographicSupportProducts::try_from_shape(frame, &line(&xs, &ys)).unwrap();
            assert_eq!(products.support.bounds(frame), Some(expected));
            assert_eq!(products.work.coordinate_visits, xs.len());
            assert_eq!(products.work.segment_connections, xs.len() - 1);
            assert_eq!(products.work.segment_intervals, 1);
        }
        let xs = [0.0, period / 3.0, -period / 3.0, 0.0];
        let coords = CoordSeq::try_from_vecs(xs.to_vec(), vec![0.0; 4], None, None).unwrap();
        let points = Shape::MultiPoint(MultiPoint::from_coordseq(coords.clone()).unwrap());
        let chain = Shape::LineString(LineSeq::try_new(coords).unwrap());
        let disconnected = GeographicSupportProducts::try_from_shape(frame, &points).unwrap();
        assert_ne!(disconnected.support.longitude, CircularInterval::Full);
        assert_eq!(disconnected.work.point_intervals, 4);
        let connected = GeographicSupportProducts::try_from_shape(frame, &chain).unwrap();
        assert_eq!(connected.support.longitude, CircularInterval::Full);
        assert_eq!(connected.work.segment_connections, 3);
        assert_eq!(connected.work.segment_intervals, 1);
    }
}

#[test]
fn full_connected_support_still_admits_later_latitude_and_errors() {
    let frame = GeographicFrame::crs84();
    let circle = line(&[0.0, 360.0], &[0.0, 0.0]);
    let tail = Shape::Point(Point::new(10.0, 70.0).unwrap());
    let products =
        GeographicSupportProducts::try_from_shapes(frame, &[circle.clone(), tail]).unwrap();
    assert_eq!(
        products.support.bounds(frame),
        Some(Bounds::new_raw(-180.0, 0.0, 180.0, 70.0))
    );
    assert_eq!(products.work.coordinate_visits, 3);
    assert_eq!(products.work.segment_connections, 1);
    let invalid = Shape::Point(Point::new_raw_xy(10.0, 100.0));
    let collection = Shape::geometry_collection(vec![circle, invalid]).unwrap();
    let Err(GeographicAdmissionError::Geographic(error)) =
        GeographicSupportProducts::try_from_shape(frame, &collection)
    else {
        panic!("Full longitude support must still admit the invalid tail");
    };
    assert!(matches!(
        error.source,
        GeographicError::LatitudeOutOfRange { .. }
    ));
    assert_eq!(error.location.members.as_slice(), &[1]);
    assert_eq!(error.location.coordinate, 0);
}

#[test]
fn shared_point_and_polygon_columns_keep_ring_gate_both_orders() {
    let frame = GeographicFrame::crs84();
    let coords =
        CoordSeq::try_from_vecs(vec![0.0, 120.0, -120.0, 0.0], vec![0.0; 4], None, None).unwrap();
    let points = Shape::MultiPoint(MultiPoint::from_coordseq(coords.clone()).unwrap());
    let polygon = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(coords).unwrap(),
        Vec::new(),
    ));
    for (members, position) in [
        (vec![points.clone(), polygon.clone()], 1),
        (vec![polygon, points], 0),
    ] {
        let collection = Shape::geometry_collection(members).unwrap();
        let Err(GeographicAdmissionError::Geographic(error)) =
            GeographicSupportProducts::try_from_shape(frame, &collection)
        else {
            panic!("coordinate admission does not replace the ring-domain gate");
        };
        assert!(matches!(
            error.source,
            GeographicError::AmbiguousFullTurnRing { .. }
        ));
        assert_eq!(error.location.members.as_slice(), &[position]);
    }
}

#[test]
fn shared_point_and_line_columns_in_one_collection_keep_connected_support_both_orders() {
    let frame = GeographicFrame::crs84();
    let coords =
        CoordSeq::try_from_vecs(vec![0.0, 120.0, -120.0, 0.0], vec![5.0; 4], None, None).unwrap();
    let points = Shape::MultiPoint(MultiPoint::from_coordseq(coords.clone()).unwrap());
    let chain = Shape::LineString(LineSeq::try_new(coords).unwrap());
    let point_support = GeographicSupportEnvelope::try_from_shape(frame, &points).unwrap();
    assert_ne!(point_support.longitude, CircularInterval::Full);
    assert_eq!(
        point_support.bounds(frame),
        Some(Bounds::new_raw(-120.0, 5.0, 120.0, 5.0))
    );
    let actual = [vec![points.clone(), chain.clone()], vec![chain, points]].map(|members| {
        let collection = Shape::geometry_collection(members).unwrap();
        let support = GeographicSupportEnvelope::try_from_shape(frame, &collection).unwrap();
        (support.longitude, support.bounds(frame))
    });
    assert_eq!(actual, [
        (
            CircularInterval::Full,
            Some(Bounds::new_raw(-180.0, 5.0, 180.0, 5.0)),
        ),
        (
            CircularInterval::Full,
            Some(Bounds::new_raw(-180.0, 5.0, 180.0, 5.0)),
        ),
    ]);
}

#[test]
fn shared_point_and_segment_runs_keep_distinct_support_owners() {
    let frame = native_frames()[0];
    let coords =
        CoordSeq::try_from_vecs(vec![0.0, 120.0, 240.0, 360.0], vec![0.0; 4], None, None).unwrap();
    let points = Shape::MultiPoint(MultiPoint::from_coordseq(coords.clone()).unwrap());
    let line = Shape::LineString(LineSeq::try_new(coords).unwrap());
    let products = GeographicSupportProducts::try_from_shapes(frame, &[points, line]).unwrap();

    assert_eq!(products.support.longitude, CircularInterval::Full);
    assert_eq!(products.work.point_intervals, 4);
    assert_eq!(products.work.segment_connections, 3);
    assert_eq!(products.work.segment_intervals, 1);
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
