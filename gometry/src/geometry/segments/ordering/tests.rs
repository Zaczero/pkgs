use self::support::RemainingWork;
use super::{compare_along_segment, try_sort_by, try_sort_by_with_work};
use crate::error::ErrorKind;
use crate::geometry::{GeometryErrorKind, Segment, XY};

mod support;

#[test]
fn fallible_sort_is_stable_and_matches_the_standard_sort() {
    for len in (0..=70).chain([255, 256, 257, 1000]) {
        let items: Vec<(u32, usize)> = (0..len)
            .map(|index| ((index as u32).wrapping_mul(2_654_435_761) % 7, index))
            .collect();
        let mut expected = items.clone();
        #[expect(
            clippy::disallowed_methods,
            reason = "The standard stable sort is the independent oracle for equal-key ordering."
        )]
        expected.sort_by_key(|item| item.0);
        let mut sorted = items;
        try_sort_by(&mut sorted, |left, right| Ok(left.0.cmp(&right.0))).unwrap();
        assert_eq!(sorted, expected, "length {len}");
    }

    let mut tied = [
        (1, 'A'),
        (1, 'B'),
        (1, 'C'),
        (1, 'D'),
        (1, 'E'),
        (1, 'F'),
        (1, 'G'),
        (1, 'H'),
        (2, 'I'),
        (2, 'J'),
        (2, 'K'),
        (2, 'L'),
        (2, 'M'),
        (2, 'N'),
        (2, 'O'),
        (2, 'P'),
        (1, 'Q'),
        (2, 'R'),
    ];
    try_sort_by(&mut tied, |left, right| Ok(left.0.cmp(&right.0))).unwrap();
    assert_eq!(tied, [
        (1, 'A'),
        (1, 'B'),
        (1, 'C'),
        (1, 'D'),
        (1, 'E'),
        (1, 'F'),
        (1, 'G'),
        (1, 'H'),
        (1, 'Q'),
        (2, 'I'),
        (2, 'J'),
        (2, 'K'),
        (2, 'L'),
        (2, 'M'),
        (2, 'N'),
        (2, 'O'),
        (2, 'P'),
        (2, 'R'),
    ]);
}

#[test]
fn synthetic_work_error_precedes_a_sort_comparison() {
    let mut items = [(2_u8, 'A'), (1, 'B')];
    let original = items;
    let mut comparisons = 0;
    let mut work = RemainingWork(0);

    let result = try_sort_by_with_work(&mut items, &mut work, |left, right, _work| {
        comparisons += 1;
        Ok(left.0.cmp(&right.0))
    });

    let error = result.unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
    ));
    assert_eq!(comparisons, 0);
    assert_eq!(items, original);
}

#[test]
fn synthetic_work_error_precedes_a_sort_swap() {
    let mut items = [(2_u8, 'A'), (1, 'B')];
    let original = items;
    let mut comparisons = 0;
    let mut work = RemainingWork(1);

    let result = try_sort_by_with_work(&mut items, &mut work, |left, right, _work| {
        comparisons += 1;
        Ok(left.0.cmp(&right.0))
    });

    let error = result.unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
    ));
    assert_eq!(comparisons, 1);
    assert_eq!(items, original);
}

#[test]
fn fallible_sort_stops_at_the_first_comparison_error() {
    let mut items: Vec<u32> = (0..100).rev().collect();
    let mut comparisons = 0;
    let result = try_sort_by(&mut items, |left, right| {
        comparisons += 1;
        if comparisons == 40 {
            return Err(GeometryErrorKind::voronoi("stop"));
        }
        Ok(left.cmp(right))
    });
    assert!(result.is_err());
    assert_eq!(comparisons, 40);
}

#[test]
fn fallible_sort_stops_at_a_merge_error_with_a_valid_item_permutation() {
    let mut items = [
        (1_u8, 101_u16),
        (3, 102),
        (5, 103),
        (7, 104),
        (9, 105),
        (11, 106),
        (13, 107),
        (15, 108),
        (17, 109),
        (19, 110),
        (21, 111),
        (23, 112),
        (25, 113),
        (27, 114),
        (29, 115),
        (31, 116),
        (0, 201),
        (2, 202),
        (4, 203),
        (6, 204),
        (8, 205),
        (10, 206),
        (12, 207),
        (14, 208),
        (16, 209),
        (18, 210),
        (20, 211),
        (22, 212),
        (24, 213),
        (26, 214),
        (28, 215),
        (30, 216),
    ];
    let expected_records = [
        (1, 101),
        (3, 102),
        (5, 103),
        (7, 104),
        (9, 105),
        (11, 106),
        (13, 107),
        (15, 108),
        (17, 109),
        (19, 110),
        (21, 111),
        (23, 112),
        (25, 113),
        (27, 114),
        (29, 115),
        (31, 116),
        (0, 201),
        (2, 202),
        (4, 203),
        (6, 204),
        (8, 205),
        (10, 206),
        (12, 207),
        (14, 208),
        (16, 209),
        (18, 210),
        (20, 211),
        (22, 212),
        (24, 213),
        (26, 214),
        (28, 215),
        (30, 216),
    ];
    let mut observed_merge_pairs = Vec::new();
    let mut error_observed = false;
    let mut post_error_comparisons = 0;

    let result = try_sort_by(&mut items, |left, right| {
        if error_observed {
            post_error_comparisons += 1;
            return Err(GeometryErrorKind::voronoi("comparison after merge error"));
        }
        let cross_run = (101..=116).contains(&left.1) && (201..=216).contains(&right.1);
        if cross_run {
            let pair = (left.1, right.1);
            observed_merge_pairs.push(pair);
            if (left.0, left.1, right.0, right.1) == (3, 102, 2, 202) {
                error_observed = true;
                return Err(GeometryErrorKind::voronoi("merge comparator stop"));
            }
        }
        Ok(left.0.cmp(&right.0))
    });

    let error = result.expect_err("the selected cross-run comparison must fail");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Voronoi(message))
            if message.as_ref() == "merge comparator stop"
    ));
    assert!(
        error_observed,
        "the comparator must reach the merge error pair"
    );
    assert_eq!(observed_merge_pairs, [(101, 201), (101, 202), (102, 202)]);
    assert_eq!(
        post_error_comparisons, 0,
        "the first comparator error must stop further callback calls"
    );

    assert_eq!(items.len(), expected_records.len());
    for (key, source_tag) in expected_records {
        assert_eq!(
            items
                .iter()
                .filter(|record| **record == (key, source_tag))
                .count(),
            1,
            "record ({key}, {source_tag})"
        );
    }
}

#[rstest::rstest]
#[case::segment_start_xy_new_0_0_0_0_end_xy_new_4_0_0_0(
    Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(4.0, 0.0),
    },
    XY::new(1.0, 0.0),
    XY::new(3.0, 0.0)
)]
#[case::segment_start_xy_new_0_0_0_0_end_xy_new_0_0_4_0(
    Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(0.0, 4.0),
    },
    XY::new(0.0, 1.0),
    XY::new(0.0, 3.0)
)]
#[case::segment_start_xy_new_4_0_0_0_end_xy_new_0_0_0_0(
    Segment {
        start: XY::new(4.0, 0.0),
        end: XY::new(0.0, 0.0),
    },
    XY::new(3.0, 0.0),
    XY::new(1.0, 0.0)
)]
#[case::segment_start_xy_new_4_0_0_0_end_xy_new_0_0_0_0_02521fe3(
    Segment {
        start: XY::new(-4.0, 0.0),
        end: XY::new(0.0, 0.0),
    },
    XY::new(-3.0, 0.0),
    XY::new(-1.0, 0.0)
)]
#[case::segment_start_xy_new_0_0_0_0_end_xy_new_4_0_f64_from_bits_1_0_0(
    Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(4.0 * f64::from_bits(1), 0.0),
    },
    XY::new(f64::from_bits(1), 0.0),
    XY::new(3.0 * f64::from_bits(1), 0.0)
)]
#[case::segment_start_xy_new_f64_max_0_0_end_xy_new_f64_max_0_0(
    Segment {
        start: XY::new(-f64::MAX, 0.0),
        end: XY::new(f64::MAX, 0.0),
    },
    XY::new(-f64::MAX / 2.0, 0.0),
    XY::new(f64::MAX / 2.0, 0.0)
)]
fn cuts_sort_by_directed_coordinate_at_float_extremes(
    #[case] segment: Segment,
    #[case] first: XY,
    #[case] second: XY,
) {
    assert_eq!(
        compare_along_segment(segment, first, second),
        std::cmp::Ordering::Less
    );
}

#[test]
fn cut_order_canonicalizes_signed_zero_before_secondary_ties() {
    let diagonal = Segment {
        start: XY::new(-2.0, 0.0),
        end: XY::new(2.0, 4.0),
    };
    assert_eq!(
        compare_along_segment(diagonal, XY::new(0.0, 1.0), XY::new(-0.0, 3.0)),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_along_segment(diagonal, XY::new(-0.0, 2.0), XY::new(0.0, 2.0)),
        std::cmp::Ordering::Equal
    );

    let descending = Segment {
        start: XY::new(0.0, 4.0),
        end: XY::new(4.0, 0.0),
    };
    assert_eq!(
        compare_along_segment(descending, XY::new(2.0, 3.0), XY::new(2.0, 1.0)),
        std::cmp::Ordering::Less
    );
}
