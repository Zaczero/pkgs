use super::*;

#[test]
fn bowtie_polygon_keeps_both_filled_lobes_in_s2_cover() {
    let shell = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(2.0, 2.0),
        Point::new_raw_xy(0.0, 2.0),
        Point::new_raw_xy(2.0, 0.0),
        Point::new_raw_xy(0.0, 0.0),
    ];
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&shell).unwrap()).unwrap(),
        Vec::new(),
    ));
    let left_lobe = Point::new_raw_xy(0.5, 1.5);
    let right_lobe = Point::new_raw_xy(1.5, 0.5);
    assert!(source.covers_point(left_lobe));
    assert!(source.covers_point(right_lobe));

    let covering = fixed(8).cover_identity(&source).expect("bowtie cover");
    let outer = covering.outer();
    assert!(
        outer.contains(
            &CellId::from_lonlat(left_lobe.x, left_lobe.y)
                .parent(8)
                .unwrap()
        )
    );
    assert!(
        outer.contains(
            &CellId::from_lonlat(right_lobe.x, right_lobe.y)
                .parent(8)
                .unwrap()
        )
    );
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_box() {
    assert_relate_oracle_parity("box", &box_shape(13.0, 52.0, 14.0, 53.0), &[6, 8, 10]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_holed_polygon() {
    assert_relate_oracle_parity("hole", &polygon_with_hole(), &[5, 6]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_line() {
    let source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-3.0, -2.0),
                Point::new_raw_xy(4.0, 3.0),
            ])
            .unwrap(),
        )
        .expect("test line is valid"),
    );
    assert_relate_oracle_parity("line", &source, &[5, 7]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_mixed_dimension_collection() {
    assert_relate_oracle_parity("mixed", &mixed_dimension_collection(), &[5, 7]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_an_adjacent_areal_collection() {
    assert_relate_oracle_parity("adjacent-areal", &adjacent_areal_collection(), &[6, 8]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_an_overlapping_areal_collection() {
    assert_relate_oracle_parity("overlapping-areal", &overlapping_areal_collection(), &[
        6, 8,
    ]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_nested_areal_collection() {
    assert_relate_oracle_parity("nested-areal", &nested_areal_collection(), &[8, 10]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_thin_boundary() {
    assert_relate_oracle_parity("thin-boundary", &box_shape(-0.25, -0.05, 0.25, 0.05), &[
        6, 8, 10,
    ]);
}

#[test]
fn native_coverer_matches_relate_oracle_across_the_antimeridian() {
    assert_relate_oracle_parity("antimeridian", &antimeridian_seam_source(), &[4, 6]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_pole_touching_line() {
    let source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-60.0, 80.0),
                Point::new_raw_xy(-60.0, 90.0),
            ])
            .unwrap(),
        )
        .expect("pole line is valid"),
    );
    assert_relate_oracle_parity("pole-line", &source, &[2, 4, 6]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_full_world_band() {
    let source = box_shape(-180.0, -80.0, 180.0, 80.0);
    assert_relate_oracle_parity("full-world-band", &source, &[1, 2, 3]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_retraced_and_collapsed_support() {
    let retraced = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-3.0, -2.0),
                Point::new_raw_xy(4.0, 3.0),
                Point::new_raw_xy(-3.0, -2.0),
                Point::new_raw_xy(4.0, 3.0),
            ])
            .unwrap(),
        )
        .expect("retraced line is valid"),
    );
    assert_relate_oracle_parity("retraced-line", &retraced, &[4, 6]);

    let collapsed = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(12.0, 13.0),
                Point::new_raw_xy(12.0, 13.0),
                Point::new_raw_xy(12.0, 13.0),
            ])
            .unwrap(),
        )
        .expect("collapsed line is valid"),
    );
    assert_relate_oracle_parity("collapsed-line", &collapsed, &[4, 6, 8]);
}

fn assert_source_permutations_are_deterministic(level: u8, rule: CoverRule) {
    let left = box_shape(-1.0, -1.0, 0.03, 1.0);
    let right = box_shape(0.03, -1.0, 1.0, 1.0);
    let first = Shape::geometry_collection(vec![left.clone(), right.clone()])
        .expect("first collection is valid");
    let second =
        Shape::geometry_collection(vec![right, left]).expect("permuted collection is valid");

    let first_cover = fixed(level)
        .cover_identity_for_rule(&first, rule)
        .expect("first permutation covers");
    let second_cover = fixed(level)
        .cover_identity_for_rule(&second, rule)
        .expect("second permutation covers");
    assert_eq!(
        first_cover.into_cells(),
        second_cover.into_cells(),
        "level {level} rule {rule:?}"
    );
}

#[rstest::rstest]
#[case::source_permutations_level_four_overlap(4, CoverRule::Overlap)]
#[case::source_permutations_level_four_within(4, CoverRule::Within)]
#[case::source_permutations_level_four_center(4, CoverRule::Center)]
#[case::source_permutations_level_six_overlap(6, CoverRule::Overlap)]
#[case::source_permutations_level_six_within(6, CoverRule::Within)]
#[case::source_permutations_level_six_center(6, CoverRule::Center)]
#[case::source_permutations_level_eight_overlap(8, CoverRule::Overlap)]
#[case::source_permutations_level_eight_within(8, CoverRule::Within)]
#[case::source_permutations_level_eight_center(8, CoverRule::Center)]
fn source_permutations_are_deterministic(#[case] level: u8, #[case] rule: CoverRule) {
    assert_source_permutations_are_deterministic(level, rule);
}
