#[test]
fn seam_run_between_opposite_sides_belongs_to_its_interior_side() {
    // Fiji's dissolved outline: it reaches the seam from the east at
    // -15.5, runs south along it to -19.15, and leaves to the west.
    let shell = [
        (-180.0, -19.153_409),
        (-179.963_319_5, -15.507_781_7),
        (-178.980_216_6, -15.967_318_2),
        (-178.045_034, -17.836_819_8),
        (-178.011_665, -19.811_436_8),
        (-178.928_670_8, -21.213_499_6),
        (-179.094_029_1, -20.818_838_7),
        (180.0, -19.800_520_5),
        (179.648_239_9, -19.405_357_3),
        (176.805_793_9, -17.730_552_9),
        (177.194_871, -16.601_579),
        (180.0, -15.508_868_1),
    ];
    let reversed: Vec<(f64, f64)> = shell.iter().rev().copied().collect();
    let area = split_crs84(&polygon(&shell, &[])).area();
    for ring in [&shell[..], &reversed[..]] {
        assert_seam_spellings_split_alike(ring, &[], 2, area);
    }
}

#[test]
fn hole_notch_touching_the_seam_trims_its_side() {
    let shell = [
        (170.0, -10.0),
        (-170.0, -10.0),
        (-170.0, 10.0),
        (170.0, 10.0),
    ];
    let notch = [(175.0, 0.0), (180.0, 0.0), (180.0, 5.0), (175.0, 5.0)];
    assert_seam_spellings_split_alike(&shell, &[&notch], 2, 400.0 - 25.0);
}

#[test]
fn seam_edge_of_a_one_sided_polygon_keeps_it_whole() {
    let shell = [(170.0, 0.0), (180.0, 0.0), (180.0, 10.0), (170.0, 10.0)];
    assert_seam_spellings_split_alike(&shell, &[], 1, 100.0);
}

#[test]
fn crs84_zero_to_full_turn_band_rewrites() {
    let shape = Shape::Polygon(Polygon::new(
        closed_ring(&[0.0, 360.0, 360.0, 0.0, 0.0], &[
            -30.0, -30.0, 30.0, 30.0, -30.0,
        ]),
        Vec::new(),
    ));
    let split = split_crs84(&shape);
    assert_ne!(split, shape);
    let expected = Shape::Polygon(Polygon::new(
        closed_ring(
            &[
                -180.0, -90.0, 0.0, 90.0, 180.0, 180.0, 90.0, 0.0, -90.0, -180.0, -180.0,
            ],
            &[
                -30.0, -30.0, -30.0, -30.0, -30.0, 30.0, 30.0, 30.0, 30.0, 30.0, -30.0,
            ],
        ),
        Vec::new(),
    ));
    assert_eq!(split, expected);
}

fn assert_split_polar_annulus_band(longitude: f64) {
    let latitude_ring = |latitude| {
        [
            (0.0, latitude),
            (60.0, latitude),
            (120.0, latitude),
            (180.0, latitude),
            (-120.0, latitude),
            (-60.0, latitude),
        ]
    };
    let shell = latitude_ring(70.0);
    let hole = latitude_ring(80.0);
    let source = polygon(&shell, &[&hole]);
    let split = split_crs84(&source);
    assert_eq!(split.validate().unwrap(), None, "{split:?}");
    let source_plan = crate::geometry::PreparedPlan::transient(&split);
    let source_cache = crate::geometry::FrameDependentCaches::default();
    let frame = GeographicFrame::crs84();
    let source_prepared =
        crate::geometry::GeographicPrepared::try_new(&source_plan, &source_cache, frame).unwrap();
    for (latitude, expected) in [(75.0, true), (89.0, false), (65.0, false)] {
        let point = Shape::Point(Point::new_raw_xy(longitude, latitude));
        let point_plan = crate::geometry::PreparedPlan::transient(&point);
        let point_cache = crate::geometry::FrameDependentCaches::default();
        let point_prepared =
            crate::geometry::GeographicPrepared::try_new(&point_plan, &point_cache, frame).unwrap();
        let relation =
            crate::geometry::graph_relate_data_geographic(source_prepared, point_prepared).unwrap();
        assert_eq!(
            relation.is_contains(),
            expected,
            "({longitude}, {latitude}) in {split:?}"
        );
    }
}

#[test]
fn split_polar_annulus_keeps_the_band_at_the_central_meridian() {
    assert_split_polar_annulus_band(0.0);
}

#[test]
fn split_polar_annulus_keeps_the_band_at_the_east_seam() {
    assert_split_polar_annulus_band(180.0);
}

#[test]
fn split_polar_annulus_keeps_the_band_at_the_west_seam() {
    assert_split_polar_annulus_band(-180.0);
}
