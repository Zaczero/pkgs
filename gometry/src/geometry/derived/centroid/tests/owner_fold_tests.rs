use super::*;
use crate::geometry::{CoordinateAxes, LineSeq, MOrdinate, ZOrdinate};

fn assert_ordinary_point(point: Point, expected: (f64, f64)) {
    // These short fixtures have at most eight sites or one five-vertex ring.
    // Allow the algebraic measurement reductions to reassociate their operations.
    approx::assert_relative_eq!(
        point.x,
        expected.0,
        epsilon = 128.0 * f64::EPSILON,
        max_relative = 128.0 * f64::EPSILON
    );
    approx::assert_relative_eq!(
        point.y,
        expected.1,
        epsilon = 128.0 * f64::EPSILON,
        max_relative = 128.0 * f64::EPSILON
    );
}

fn polygon(x: f64, y: f64, side: f64) -> Shape {
    let points = [
        Point::new_raw_xy(x, y),
        Point::new_raw_xy(x + side, y),
        Point::new_raw_xy(x + side, y + side),
        Point::new_raw_xy(x, y + side),
        Point::new_raw_xy(x, y),
    ];
    Shape::Polygon(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

fn line(points: &[(f64, f64)]) -> Shape {
    let values: Vec<_> = points
        .iter()
        .map(|&(x, y)| Point::new_raw_xy(x, y))
        .collect();
    Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&values).unwrap()).unwrap())
}

fn repeated(mut shape: Shape, depth: usize) -> Shape {
    for _ in 0..depth {
        shape = Shape::geometry_collection(vec![shape.clone(), shape]).unwrap();
    }
    shape
}

#[test]
fn centroid_owner_map_keeps_four_entries_inline_before_promoting() {
    let (mut owners, inline_allocations) = crate::test_support::allocation_count(|| {
        let mut owners = CentroidOwnerMap::<u8, u8, CENTROID_ADMISSION_INLINE_OWNERS>::default();
        for key in 0..CENTROID_ADMISSION_INLINE_OWNERS as u8 {
            owners.insert_new(&key, key * 2);
        }
        owners
    });

    assert_eq!(inline_allocations, 0);
    for key in 0..CENTROID_ADMISSION_INLINE_OWNERS as u8 {
        assert_eq!(owners.get(&key), Some(&(key * 2)));
    }

    let ((), promotion_allocations) = crate::test_support::allocation_count(|| {
        let key = CENTROID_ADMISSION_INLINE_OWNERS as u8;
        owners.insert_new(&key, key * 2);
    });

    assert!(promotion_allocations > 0);
    for key in 0..=CENTROID_ADMISSION_INLINE_OWNERS as u8 {
        assert_eq!(owners.get(&key), Some(&(key * 2)));
    }
}

fn point_from(shape: &Shape) -> Point {
    let Shape::Point(point) = shape.centroid().unwrap() else {
        panic!("centroid returns a point");
    };
    point
}

#[rstest::rstest]
#[case::depth_8(8)]
#[case::depth_12(12)]
#[case::depth_128(128)]
fn exact_owner_work_reuses_shared_collection_slices(#[case] depth: usize) {
    let polygon = polygon(0.0, 0.0, 1.0);
    let line = line(&[(0.0, 0.0), (3.0, 4.0)]);
    let multi_point = Shape::multi_point(
        CoordinateAxes::XYZM,
        CoordSeq::try_from_points(&[
            Point::new_axes(2.0, -4.0, ZOrdinate(Some(10.0)), MOrdinate(Some(20.0))).unwrap(),
            Point::new_axes(6.0, 8.0, ZOrdinate(Some(-10.0)), MOrdinate(Some(-20.0))).unwrap(),
        ])
        .unwrap(),
    );

    let cases = [
        (
            repeated(polygon, depth),
            CentroidDimension::Areal,
            (0.5, 0.5),
            10,
        ),
        (
            repeated(line, depth),
            CentroidDimension::Lineal,
            (1.5, 2.0),
            4,
        ),
        (
            repeated(multi_point, depth),
            CentroidDimension::Puntal,
            (4.0, 2.0),
            4,
        ),
    ];
    for (shape, tier, expected, source_xy) in cases {
        let (centroid, work) = exact_owner_centroid_with_work(&shape, tier).unwrap();
        let centroid = centroid.expect("positive tier measure");
        assert_eq!(centroid, expected);
        let expected_owner_visits = depth
            .checked_add(2)
            .expect("test depth fits its owner count");
        let leaf_members = match tier {
            CentroidDimension::Areal => 2,
            CentroidDimension::Lineal => 0,
            CentroidDimension::Puntal => 4,
        };
        let expected_member_slots = depth
            .checked_mul(2)
            .and_then(|slots| slots.checked_add(leaf_members))
            .expect("test depth fits its member count");
        assert_eq!(work.owner_visits, expected_owner_visits);
        assert_eq!(work.member_slots, expected_member_slots);
        assert_eq!(work.source_xy, source_xy);
    }
}

#[rstest::rstest]
#[case::depth_4(4)]
#[case::depth_8(8)]
fn repeated_shared_line_dags_wait_for_the_repeat_work_budget(#[case] depth: usize) {
    let source = line(&[(0.0, 0.0), (2.0, 0.0)]);

    let shape = repeated(source, depth);
    let mut admission = None;
    let outcome = visit_lineal_owner_runs(&shape, &mut admission, &mut |_, _| false);
    assert!(
        admission
            .as_ref()
            .is_some_and(|admission| admission.repeated_owner)
    );
    let repeated_work = admission.as_ref().unwrap().repeated_work;
    if depth == 4 {
        assert!(matches!(outcome, CentroidAttempt::Ready(())));
        assert!(repeated_work <= CENTROID_FAST_REPEAT_WORK_BUDGET);
    } else {
        assert!(matches!(outcome, CentroidAttempt::ExactAtRoot(_)));
        assert!(repeated_work > CENTROID_FAST_REPEAT_WORK_BUDGET);
    }
    assert_ordinary_point(point_from(&shape), (1.0, 0.0));
}

#[test]
fn puntal_dags_replay_occurrences_until_the_repeat_work_budget() {
    let source = Shape::multi_point(
        CoordinateAxes::XY,
        CoordSeq::try_from_points(&[Point::new_raw_xy(2.0, -4.0), Point::new_raw_xy(6.0, 8.0)])
            .unwrap(),
    );

    let small = repeated(source.clone(), 2);
    let mut admission = None;
    let mut staged = 0;
    assert!(matches!(
        visit_fallback_points(&small, &mut admission, &mut |_, _| staged += 1),
        CentroidAttempt::Ready(())
    ));
    assert!(
        admission
            .as_ref()
            .is_some_and(|admission| admission.repeated_owner)
    );
    assert!(admission.as_ref().unwrap().repeated_work <= CENTROID_FAST_REPEAT_WORK_BUDGET);
    assert_eq!(staged, 8);
    assert_ordinary_point(point_from(&small), (4.0, 2.0));

    let large = repeated(source, 8);
    let mut admission = None;
    let mut staged = 0;
    assert!(matches!(
        visit_fallback_points(&large, &mut admission, &mut |_, _| staged += 1),
        CentroidAttempt::ExactAtRoot(_)
    ));
    assert!(admission.as_ref().unwrap().repeated_work > CENTROID_FAST_REPEAT_WORK_BUDGET);
    assert!(staged < 2 * (1 << 8));
    assert_eq!((point_from(&large).x, point_from(&large).y), (4.0, 2.0));
}

#[test]
fn exact_at_root_precedes_nested_infinite_area_replay() {
    let shape = repeated(polygon(0.0, 0.0, 1.0e200), 12);
    let mut admission = None;
    assert!(matches!(
        areal_centroid_summary(&shape, &mut admission),
        CentroidAttempt::ExactAtRoot(_)
    ));
    let point = point_from(&shape);
    assert_eq!(point.x, 5.0e199);
    assert_eq!(point.y, 5.0e199);
}

#[test]
fn external_polygon_alias_does_not_start_collection_admission() {
    let source = polygon(0.0, 0.0, 1.0);
    let external_alias = Shape::geometry_collection(vec![source.clone()]).unwrap();
    let mut admission = None;
    let CentroidAttempt::Ready(Some((area, x, y))) =
        areal_centroid_summary(&external_alias, &mut admission)
    else {
        panic!("one reachable externally aliased child remains on the fast path");
    };
    approx::assert_relative_eq!(area, 1.0, max_relative = 64.0 * f64::EPSILON);
    assert_ordinary_point(Point::new_raw_xy(x, y), (0.5, 0.5));
    assert!(admission.is_none());
    assert_ordinary_point(point_from(&source), (0.5, 0.5));
    drop(external_alias);
}

#[test]
fn external_collection_alias_occurring_once_keeps_its_centroid() {
    let aliased_collection = Shape::geometry_collection(vec![polygon(0.0, 0.0, 1.0)]).unwrap();
    let external_collection_alias = aliased_collection.clone();
    let root = Shape::geometry_collection(vec![aliased_collection]).unwrap();
    let mut admission = None;
    let CentroidAttempt::Ready(Some((area, x, y))) = areal_centroid_summary(&root, &mut admission)
    else {
        panic!("one externally aliased collection occurrence keeps the fast path");
    };
    approx::assert_relative_eq!(area, 1.0, max_relative = 64.0 * f64::EPSILON);
    assert_ordinary_point(Point::new_raw_xy(x, y), (0.5, 0.5));
    assert!(admission.is_some());
    assert_ordinary_point(point_from(&root), (0.5, 0.5));
    drop(external_collection_alias);
}

#[test]
fn exact_fold_reuses_shared_collection_owner_across_siblings() {
    let shared = Shape::geometry_collection(vec![polygon(0.0, 0.0, 1.0)]).unwrap();
    let left = Shape::geometry_collection(vec![shared.clone(), polygon(20.0, 0.0, 1.0)]).unwrap();
    let right = Shape::geometry_collection(vec![shared, polygon(40.0, 0.0, 1.0)]).unwrap();
    let siblings = Shape::geometry_collection(vec![left, right]).unwrap();
    let (centroid, work) =
        exact_owner_centroid_with_work(&siblings, CentroidDimension::Areal).unwrap();
    assert_eq!(centroid, Some((15.5, 0.5)));
    assert!(work.cache_hits > 0);
}

#[test]
fn exact_fold_counts_late_repeated_shared_owners_in_every_tier() {
    let area_a = Shape::geometry_collection(vec![polygon(0.5, -0.5, 1.0)]).unwrap();
    let area_b = Shape::geometry_collection(vec![polygon(20.5, -0.5, 1.0)]).unwrap();
    let area_c = Shape::geometry_collection(vec![polygon(40.5, -0.5, 1.0)]).unwrap();
    let area = Shape::geometry_collection(vec![
        area_a.clone(),
        area_b.clone(),
        area_c.clone(),
        area_b.clone(),
        area_a,
        area_b,
    ])
    .unwrap();

    let line_a = Shape::geometry_collection(vec![line(&[(0.0, 0.0), (2.0, 0.0)])]).unwrap();
    let line_b = Shape::geometry_collection(vec![line(&[(20.0, 0.0), (22.0, 0.0)])]).unwrap();
    let line_c = Shape::geometry_collection(vec![line(&[(40.0, 0.0), (42.0, 0.0)])]).unwrap();
    let lineal = Shape::geometry_collection(vec![
        line_a.clone(),
        line_b.clone(),
        line_c.clone(),
        line_b.clone(),
        line_a,
        line_b,
    ])
    .unwrap();

    let point_a =
        Shape::geometry_collection(vec![Shape::Point(Point::new_raw_xy(1.0, 0.0))]).unwrap();
    let point_b =
        Shape::geometry_collection(vec![Shape::Point(Point::new_raw_xy(21.0, 0.0))]).unwrap();
    let point_c =
        Shape::geometry_collection(vec![Shape::Point(Point::new_raw_xy(41.0, 0.0))]).unwrap();
    let puntal = Shape::geometry_collection(vec![
        point_a.clone(),
        point_b.clone(),
        point_c.clone(),
        point_b.clone(),
        point_a,
        point_b,
    ])
    .unwrap();

    for (shape, tier) in [
        (&area, CentroidDimension::Areal),
        (&lineal, CentroidDimension::Lineal),
        (&puntal, CentroidDimension::Puntal),
    ] {
        let (centroid, _) = exact_owner_centroid_with_work(shape, tier).unwrap();
        assert_eq!(centroid, Some((53.0 / 3.0, 0.0)));
    }

    // Keep the single-occurrence owners shared until every tier has folded.
    drop((area_c, line_c, point_c));
}

#[test]
fn unshared_collection_members_construct_no_tier_admission() {
    let shape = Shape::geometry_collection(vec![
        polygon(0.0, 0.0, 1.0),
        line(&[(10.0, 0.0), (12.0, 0.0)]),
    ])
    .unwrap();
    let Shape::GeometryCollection(collection) = &shape else {
        unreachable!("fixture is a collection")
    };
    assert!(!collection.members_are_shared());

    let mut areal_admission = None;
    let CentroidAttempt::Ready(Some((area, x, y))) =
        areal_centroid_summary(&shape, &mut areal_admission)
    else {
        panic!("unshared members stay on the ordinary areal route");
    };
    approx::assert_relative_eq!(area, 1.0, max_relative = 64.0 * f64::EPSILON);
    assert_ordinary_point(Point::new_raw_xy(x, y), (0.5, 0.5));
    assert!(areal_admission.is_none());

    let mut lineal_admission = None;
    assert!(matches!(
        visit_lineal_owner_runs(&shape, &mut lineal_admission, &mut |_, _| false),
        CentroidAttempt::Ready(())
    ));
    assert!(lineal_admission.is_none());

    let mut puntal_admission = None;
    assert!(matches!(
        visit_fallback_points(&shape, &mut puntal_admission, &mut |_, _| {}),
        CentroidAttempt::Ready(())
    ));
    assert!(puntal_admission.is_none());
    assert_ordinary_point(point_from(&shape), (0.5, 0.5));
}

#[test]
fn centroid_empty_probe_memoizes_depth_128_empty_collection_dags() {
    let shape = repeated(Shape::empty_point(), 128);
    let (is_empty, work) = centroid_is_empty_with_work(&shape);
    assert!(is_empty);
    assert_eq!(work.member_slots, 256);
    assert_eq!(work.owner_visits, 129);
    assert_eq!(work.cache_entries, 127);
    assert_eq!(work.cache_hits, 127);
    assert_eq!(shape.centroid().unwrap(), Shape::empty_point());

    let unshared =
        Shape::geometry_collection(vec![Shape::empty_point(), Shape::empty_point()]).unwrap();
    let (is_empty, work) = centroid_is_empty_with_work(&unshared);
    assert!(is_empty);
    assert_eq!(work.member_slots, 0);
    assert_eq!(work.cache_entries, 0);
}

#[test]
fn centroid_empty_probe_does_not_cache_nonempty_shared_owners() {
    let shape = repeated(Shape::Point(Point::new_raw_xy(2.0, -4.0)), 4);

    let (is_empty, work) = centroid_is_empty_with_work(&shape);

    assert!(!is_empty);
    assert_eq!(work.cache_entries, 0);
    assert_eq!(work.cache_hits, 0);
}

#[test]
fn centroid_empty_probe_keeps_one_external_alias_inline() {
    let shared = Shape::geometry_collection(vec![Shape::empty_point()]).unwrap();
    let external_alias = shared.clone();
    let root = Shape::geometry_collection(vec![shared]).unwrap();

    let ((is_empty, work), probe_allocations) =
        crate::test_support::allocation_count(|| centroid_is_empty_with_work(&root));

    assert!(is_empty);
    assert_eq!(work.cache_entries, 1);
    assert_eq!(work.cache_hits, 0);
    assert_eq!(probe_allocations, 0);

    let (centroid, centroid_allocations) =
        crate::test_support::allocation_count(|| root.centroid().unwrap());
    assert_eq!(centroid, Shape::empty_point());
    assert_eq!(centroid_allocations, 0);
    drop(external_alias);
}

#[test]
fn centroid_empty_probe_keeps_late_external_alias_outside_the_tree() {
    let child = Shape::geometry_collection(vec![Shape::empty_point()]).unwrap();
    let root = Shape::geometry_collection(vec![child]).unwrap();
    let Shape::GeometryCollection(root_collection) = &root else {
        unreachable!("collection fixture")
    };
    let Shape::GeometryCollection(child_collection) = &root_collection.as_slice()[0] else {
        unreachable!("nested collection fixture")
    };
    let external_alias = Shape::GeometryCollection(child_collection.clone());
    assert!(child_collection.members_are_shared());

    let ((is_empty, work), probe_allocations) =
        crate::test_support::allocation_count(|| centroid_is_empty_with_work(&root));

    assert!(is_empty);
    assert_eq!(work.member_slots, 2);
    assert_eq!(work.cache_entries, 1);
    assert_eq!(work.cache_hits, 0);
    assert_eq!(probe_allocations, 0);
    assert_eq!(root.centroid().unwrap(), Shape::empty_point());
    drop(external_alias);
}

#[test]
fn unshared_polygon_centroid_does_not_allocate_owner_state() {
    let shape = polygon(0.0, 0.0, 1.0);

    let (centroid, allocations) =
        crate::test_support::allocation_count(|| shape.centroid().unwrap());

    assert_ordinary_point(point_from(&centroid), (0.5, 0.5));
    assert_eq!(allocations, 0);
}

#[test]
fn repeated_zero_length_line_occurrences_keep_puntal_anchors() {
    let line = line(&[(2.0, 4.0), (2.0, 4.0)]);
    let shape = Shape::geometry_collection(vec![
        line.clone(),
        line,
        Shape::Point(Point::new_raw_xy(10.0, 0.0)),
    ])
    .unwrap();

    let centroid = point_from(&shape);

    assert_ordinary_point(centroid, (14.0 / 3.0, 8.0 / 3.0));
}

#[test]
fn zero_length_far_line_does_not_move_positive_line_rescue_frame() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let maximum = f64::MAX;
    let anchor = 3.0_f64 * exact_power_of_two(970);
    let minimum = 2.0 * anchor - maximum;
    let zero = line(&[(minimum, 0.0), (minimum, 0.0)]);
    let positive = line(&[(maximum, -maximum), (maximum, maximum)]);
    for shape in [
        Shape::geometry_collection(vec![zero.clone(), positive.clone()]).unwrap(),
        Shape::geometry_collection(vec![positive, zero]).unwrap(),
    ] {
        let point = point_from(&shape);
        assert_eq!(point.x.to_bits(), maximum.to_bits());
        assert_eq!(point.y.to_bits(), 0.0_f64.to_bits());
    }
}
