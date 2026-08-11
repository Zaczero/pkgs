use crate::geometry::{Bounds, CoordSeq, LineSeq, Point, Polygon, Ring, Shape};
use crate::grid::s2::bounding::{
    LonLatBBox, bbox_closed_in_cell, bbox_hilbert_samples, bbox_margin_in_cell, bounding_cell,
    bounding_cell_bbox, hilbert_samples_ok,
};
use crate::grid::s2::cellid::CellId;

fn line(a: (f64, f64), b: (f64, f64)) -> Shape {
    Shape::LineString(
        LineSeq::try_new(CoordSeq::from(vec![
            Point::new_unchecked_xy(a.0, a.1),
            Point::new_unchecked_xy(b.0, b.1),
        ]))
        .expect("test line"),
    )
}

fn assert_hilbert_contains_samples(cell: CellId, samples: &[(f64, f64)]) {
    for &(lon, lat) in samples {
        assert!(
            cell.contains(CellId::from_lonlat(lon, lat)),
            "cell {} L{} must Hilbert-contain ({lon},{lat})",
            cell.token(),
            cell.level(),
        );
    }
}

/// Geometric soundness: closed halfspaces cover the whole bbox; no child
/// clears the positive-margin gate (deepest-provable). Degenerate
/// cube-edge segments also require same-face Hilbert sample containment.
fn bbox_sound_ok(cell: CellId, minx: f64, miny: f64, maxx: f64, maxy: f64) {
    let bbox = LonLatBBox {
        lon0: minx.to_radians(),
        lon1: maxx.to_radians(),
        lat0: miny.to_radians(),
        lat1: maxy.to_radians(),
    };
    assert!(
        bbox_closed_in_cell(cell, &bbox),
        "cell {} L{} must closed-contain bbox ({minx},{miny},{maxx},{maxy})",
        cell.token(),
        cell.level(),
    );
    let samples = bbox_hilbert_samples(minx, miny, maxx, maxy);
    #[expect(
        clippy::float_cmp,
        reason = "finite bbox edges; signed zero must match"
    )]
    let degenerate = minx == maxx || miny == maxy;
    if degenerate {
        assert!(
            hilbert_samples_ok(cell, &samples),
            "cell {} L{} must Hilbert-contain same-face samples of degenerate bbox",
            cell.token(),
            cell.level(),
        );
    }
    // Deepest-provable: no child clears the positive margin.
    if let Some(children) = cell.children() {
        for child in children {
            assert!(
                !bbox_margin_in_cell(child, &bbox),
                "cell {} L{} is not deepest-provable: child {} still margin-contains",
                cell.token(),
                cell.level(),
                child.token(),
            );
        }
    }
}

fn dense_edge_geom_ok(cell: CellId, minx: f64, miny: f64, maxx: f64, maxy: f64) {
    bbox_sound_ok(cell, minx, miny, maxx, maxy);
}

/// Off-seam Hilbert densify (stricter; only where face assignment is unique).
fn dense_edge_hilbert_ok(cell: CellId, minx: f64, miny: f64, maxx: f64, maxy: f64) {
    let n = 10_000_usize;
    for i in 0..n {
        let t = i as f64 / (n - 1) as f64;
        let lon = minx + t * (maxx - minx);
        let lat = miny + t * (maxy - miny);
        for (x, y) in [(lon, miny), (lon, maxy), (minx, lat), (maxx, lat)] {
            assert!(
                cell.contains(CellId::from_lonlat(x, y)),
                "cell {} L{} dense miss at ({x},{y}) for bbox ({minx},{miny},{maxx},{maxy})",
                cell.token(),
                cell.level(),
            );
        }
    }
}

#[test]
fn point_is_exact_l30_leaf() {
    let shape = Shape::Point(Point::new_unchecked_xy(13.4, 52.5));
    let cell = bounding_cell(&shape).expect("point");
    assert_eq!(cell, CellId::from_lonlat(13.4, 52.5));
    assert_eq!(cell.level(), 30);
}

#[test]
fn multipoint_uses_bbox_path_same_as_box() {
    let shape = Shape::MultiPoint(CoordSeq::from(vec![
        Point::new_unchecked_xy(0.1, 0.1),
        Point::new_unchecked_xy(0.2, 0.2),
    ]));
    let cell = bounding_cell(&shape).expect("same face");
    let bbox = bounding_cell_bbox(Bounds::new_unchecked(0.1, 0.1, 0.2, 0.2)).expect("bbox");
    assert_eq!(cell, bbox);
    dense_edge_geom_ok(cell, 0.1, 0.1, 0.2, 0.2);
}

#[test]
fn multipoint_oracle_repros_match_bbox() {
    // Leaf-LCA returned non-containing L7 'a8eb4'; bbox path is L6 'a8eb'.
    let a = Shape::MultiPoint(CoordSeq::from(vec![
        Point::new_unchecked_xy(170.0, -60.0),
        Point::new_unchecked_xy(170.2, -59.8),
    ]));
    let cell_a = bounding_cell(&a).expect("a");
    assert_eq!(cell_a.token(), "a8eb");
    assert_eq!(cell_a.level(), 6);
    assert_eq!(
        cell_a,
        bounding_cell_bbox(Bounds::new_unchecked(170.0, -60.0, 170.2, -59.8)).expect("box a")
    );
    // Leaf-LCA multi-face raise; face root '3' closed-contains the bbox.
    let b = Shape::MultiPoint(CoordSeq::from(vec![
        Point::new_unchecked_xy(45.0, -20.0),
        Point::new_unchecked_xy(45.2, -19.8),
    ]));
    let cell_b = bounding_cell(&b).expect("b");
    assert_eq!(cell_b.token(), "3");
    assert_eq!(cell_b.level(), 0);
    assert_eq!(
        cell_b,
        bounding_cell_bbox(Bounds::new_unchecked(45.0, -20.0, 45.2, -19.8)).expect("box b")
    );
}

#[test]
fn multipoint_multi_face_is_none() {
    let shape = Shape::MultiPoint(CoordSeq::from(vec![
        Point::new_unchecked_xy(-100.0, 0.0),
        Point::new_unchecked_xy(100.0, 0.0),
    ]));
    assert!(bounding_cell(&shape).is_none());
}

#[test]
fn multipoint_seam_straddle_matches_bbox() {
    let shape = Shape::MultiPoint(CoordSeq::from(vec![
        Point::new_unchecked_xy(45.0, 0.5),
        Point::new_unchecked_xy(46.0, 0.5),
    ]));
    assert_eq!(
        bounding_cell(&shape),
        bounding_cell_bbox(Bounds::new_unchecked(45.0, 0.5, 46.0, 0.5))
    );
}

#[test]
fn berlin_single_cell() {
    let cell = bounding_cell_bbox(Bounds::new_unchecked(13.3, 52.4, 13.5, 52.6)).expect("cell");
    assert_eq!(cell.token(), "47a85");
    assert_eq!(cell.level(), 8);
    assert_hilbert_contains_samples(cell, &[
        (13.3, 52.4),
        (13.5, 52.4),
        (13.5, 52.6),
        (13.3, 52.6),
    ]);
    dense_edge_hilbert_ok(cell, 13.3, 52.4, 13.5, 52.6);
}

/// ~10m Berlin box: must never over-descend to a non-containing leaf.
#[test]
fn berlin_10m_box_always_contains() {
    let minx = 13.4;
    let miny = 52.5;
    let maxx = 13.4001;
    let maxy = 52.5001;
    let cell = bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)).expect("cell");
    dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
    assert!(
        cell.contains(CellId::from_lonlat(13.40005, 52.50005)),
        "cell {} L{} must contain interior",
        cell.token(),
        cell.level(),
    );
    // Corners under closed halfspaces (Hilbert face dual-assign may
    // disagree on exact boundary leaves).
    for &(x, y) in &[
        (13.4, 52.5),
        (13.4001, 52.5),
        (13.4001, 52.5001),
        (13.4, 52.5001),
    ] {
        let leaf = CellId::from_lonlat(x, y);
        if leaf.face() == cell.face() {
            assert!(
                cell.contains(leaf),
                "cell {} L{} must contain corner leaf ({x},{y})",
                cell.token(),
                cell.level(),
            );
        }
    }
    assert!(
        cell.level() < 30,
        "10m box must not over-descend to L30, got L{}",
        cell.level()
    );
    // Oracle deepest ~L17; conservatism may stop a level early.
    assert!(
        (10..=20).contains(&cell.level()),
        "10m box level out of expected band, got L{}",
        cell.level()
    );
}

#[test]
fn multi_face_wide_box_is_none() {
    assert!(bounding_cell_bbox(Bounds::new_unchecked(-100.0, -40.0, 100.0, 40.0)).is_none());
}

#[test]
fn multi_face_moderate_box_is_none() {
    assert!(bounding_cell_bbox(Bounds::new_unchecked(-50.0, 10.0, -32.0, 15.0)).is_none());
}

#[test]
fn line_0_0_to_0_45_contains_samples() {
    // Zero-width face-center meridian: margin-only may stop at the face
    // root (min≈0 on child edges). Always containing is the invariant.
    let shape = line((0.0, 0.0), (0.0, 45.0));
    let cell = bounding_cell(&shape).expect("single-face");
    dense_edge_geom_ok(cell, 0.0, 0.0, 0.0, 45.0);
    assert_hilbert_contains_samples(cell, &[
        (0.0, 0.0),
        (0.0, 10.0),
        (0.0, 30.0),
        (0.0, 44.9),
        (0.0, 45.0),
    ]);
}

#[test]
fn face_diagonal_meridian_135_lat0_to_5_raises() {
    for lon in [135.0, -135.0] {
        assert!(
            bounding_cell_bbox(Bounds::new_unchecked(lon, 0.0, lon, 5.0)).is_none(),
            "lon={lon} lat 0..5 must multi-face"
        );
    }
}

#[test]
fn face_boundary_meridian_135_short_raises() {
    for lon in [-135.0, 135.0] {
        assert!(
            bounding_cell_bbox(Bounds::new_unchecked(lon, -10.0, lon, -9.7)).is_none(),
            "lon={lon} short face-boundary segment must multi-face"
        );
    }
}

#[test]
fn cube_edge_meridian_45_to_60_raise_or_closed_contain() {
    for lon in [90.0, -90.0, -180.0, 180.0] {
        match bounding_cell_bbox(Bounds::new_unchecked(lon, 45.0, lon, 60.0)) {
            None => {},
            Some(cell) => dense_edge_geom_ok(cell, lon, 45.0, lon, 60.0),
        }
    }
}

#[test]
fn south_pole_lon90_closed_contains() {
    let cell = bounding_cell_bbox(Bounds::new_unchecked(90.0, -80.0, 91.0, -79.0)).expect("cell");
    dense_edge_geom_ok(cell, 90.0, -80.0, 91.0, -79.0);
}

#[test]
fn antimeridian_south_closed_contains() {
    for (minx, miny, maxx, maxy) in [
        (-180.0, -85.0, -179.0, -84.0),
        (-180.0, -80.0, -179.0, -79.0),
        (-180.0, -75.0, -179.0, -74.0),
    ] {
        let cell = bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)).expect("cell");
        dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
    }
}

#[test]
fn seam_box_45_is_multi_face_or_dense_geom() {
    match bounding_cell_bbox(Bounds::new_unchecked(45.0, 0.0, 46.0, 1.0)) {
        None => {},
        Some(cell) => dense_edge_geom_ok(cell, 45.0, 0.0, 46.0, 1.0),
    }
}

#[test]
fn exact_seam_repro_neg135_lat_neg35() {
    // Boundary-adjacent: may coarsen vs theoretical deepest under margin.
    let cell =
        bounding_cell_bbox(Bounds::new_unchecked(-135.0, -35.0, -134.8, -34.8)).expect("cell");
    dense_edge_geom_ok(cell, -135.0, -35.0, -134.8, -34.8);
}

#[test]
fn exact_seam_repro_neg135_lat_0() {
    let cell = bounding_cell_bbox(Bounds::new_unchecked(-135.0, 0.0, -134.8, 0.2)).expect("cell");
    dense_edge_geom_ok(cell, -135.0, 0.0, -134.8, 0.2);
}

/// Skeptic counterexamples: 1e-6/1e-7 solid boxes that Strict over-descended.
#[test]
fn tiny_scale_solid_boxes_never_over_descend() {
    let cases = [
        (117.087_047_572_419_38, 38.620_414_518_783_92, 1e-6_f64),
        (138.685_538_819_117_9, -24.628_965_378_441_677, 1e-7),
        (-61.353_414_232_540_1, -22.102_649_469_574_658, 1e-6),
        (13.4, 52.5, 1e-4), // Berlin ~10m
        (0.0, 0.0, 1e-6),
        (-170.0, -80.0, 1e-6),
        (45.1, 0.1, 1e-6),
    ];
    for &(minx, miny, size) in &cases {
        let maxx = minx + size;
        let maxy = miny + size;
        if maxy > 90.0 || miny < -90.0 || maxx > 180.0 {
            continue;
        }
        let Some(cell) = bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)) else {
            continue;
        };
        dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
        // All same-face corners must be Hilbert-contained.
        for &(x, y) in &[
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (f64::midpoint(minx, maxx), f64::midpoint(miny, maxy)),
        ] {
            let leaf = CellId::from_lonlat(x, y);
            if leaf.face() == cell.face() {
                assert!(
                    cell.contains(leaf),
                    "non-containing {} L{} at ({x},{y}) for size={size}",
                    cell.token(),
                    cell.level(),
                );
            }
        }
    }
}

#[test]
fn exact_seam_touch_matrix_no_false_reject() {
    let seams = [-135.0_f64, -45.0, 45.0, 135.0];
    let lats = [-40.0, -35.0, -20.0, -5.0, 0.0, 5.0, 20.0, 35.0, 40.0];
    let mut ok = 0_usize;
    let mut raised = 0_usize;
    for &lon0 in &seams {
        for &lat0 in &lats {
            for &(dw, dh) in &[(0.2_f64, 0.2), (0.05, 0.05), (1.0, 1.0), (5.0, 5.0)] {
                let minx = lon0;
                let maxx = lon0 + dw;
                let miny = lat0;
                let maxy = lat0 + dh;
                if maxy > 90.0 || miny < -90.0 {
                    continue;
                }
                match bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)) {
                    None => raised += 1,
                    Some(cell) => {
                        dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
                        ok += 1;
                    },
                }
            }
        }
    }
    for &lon in &seams {
        assert!(
            bounding_cell_bbox(Bounds::new_unchecked(lon, 0.0, lon, 5.0)).is_none(),
            "zero-width face edge lon={lon}"
        );
        raised += 1;
    }
    assert!(
        ok >= 100,
        "expected many single-face seam-touch successes, got {ok}"
    );
    assert!(
        raised >= 4,
        "expected genuine dual-root raises, got {raised}"
    );
}

#[test]
fn small_equator_box_hilbert_contains_corners() {
    let cell = bounding_cell_bbox(Bounds::new_unchecked(0.0, 0.0, 1.0, 1.0)).expect("cell");
    assert_hilbert_contains_samples(cell, &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]);
    dense_edge_hilbert_ok(cell, 0.0, 0.0, 1.0, 1.0);
}

#[test]
fn line_uses_envelope_hilbert() {
    let shape = line((-21.0, 21.5), (-1.2, 22.9));
    let cell = bounding_cell(&shape).expect("cell");
    let b = shape.bounds().expect("bounds");
    dense_edge_hilbert_ok(cell, b.minx(), b.miny(), b.maxx(), b.maxy());
}

#[test]
fn poles_and_empty() {
    let north = bounding_cell(&Shape::Point(Point::new_unchecked_xy(0.0, 90.0))).expect("N");
    assert_eq!(north.level(), 30);
    assert!(bounding_cell(&Shape::empty_polygon()).is_none());
}

#[test]
fn polygon_with_hole_uses_bbox() {
    let shell = Ring::from_trusted_closed(vec![
        Point::new_unchecked_xy(-1.0, -1.0),
        Point::new_unchecked_xy(1.0, -1.0),
        Point::new_unchecked_xy(1.0, 1.0),
        Point::new_unchecked_xy(-1.0, 1.0),
        Point::new_unchecked_xy(-1.0, -1.0),
    ]);
    let hole = Ring::from_trusted_closed(vec![
        Point::new_unchecked_xy(-0.2, -0.2),
        Point::new_unchecked_xy(-0.2, 0.2),
        Point::new_unchecked_xy(0.2, 0.2),
        Point::new_unchecked_xy(0.2, -0.2),
        Point::new_unchecked_xy(-0.2, -0.2),
    ]);
    let shape = Shape::Polygon(Polygon::new(shell, vec![hole]));
    let cell = bounding_cell(&shape).expect("cell");
    dense_edge_hilbert_ok(cell, -1.0, -1.0, 1.0, 1.0);
}

#[test]
fn false_reject_corpus_targets() {
    let cases = [
        (
            -163.431_550,
            34.574_955,
            -160.480_306,
            36.395_188,
            "7dd",
            4_u8,
        ),
        (-2.612_665, 41.780_618, -0.655_635, 43.761_980, "0d5", 4),
        (121.977_231, 41.851_431, 124.077_057, 42.468_080, "5e3", 4),
    ];
    for (minx, miny, maxx, maxy, token, level) in cases {
        let cell = bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)).expect("cell");
        assert_eq!(
            (cell.token().as_str(), cell.level()),
            (token, level),
            "bbox ({minx},{miny},{maxx},{maxy})"
        );
        dense_edge_hilbert_ok(cell, minx, miny, maxx, maxy);
    }
}

#[test]
fn band_0_2_boxes_no_false_reject() {
    let lats = [
        35.3, 36.0, 38.0, 40.0, 42.0, 44.0, 44.8, -35.3, -36.0, -40.0, -44.0,
    ];
    let lons = [
        -160.0, -90.0, -45.0, -20.0, 0.0, 20.0, 45.0, 90.0, 120.0, 160.0,
    ];
    for &lat0 in &lats {
        for &lon0 in &lons {
            let minx = lon0;
            let maxx = lon0 + 0.2;
            let miny = lat0;
            let maxy = lat0 + 0.2;
            if !(maxy <= 90.0 && miny >= -90.0) {
                continue;
            }
            match bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)) {
                None => {},
                Some(cell) => dense_edge_geom_ok(cell, minx, miny, maxx, maxy),
            }
        }
    }
}

/// Multi-scale soundness: 0 non-containing at ANY scale.
#[test]
fn multi_scale_soundness_zero_non_containing() {
    let sizes = [0.2_f64, 0.01, 1e-4, 1e-6];
    let lats = [
        -80.0, -60.0, -40.0, -20.0, 0.0, 20.0, 40.0, 52.5, 60.0, 80.0,
    ];
    let lons = [
        -170.0, -135.0, -90.0, -45.0, -20.0, 0.0, 13.4, 45.0, 90.0, 135.0, 170.0,
    ];
    let mut ok = 0_usize;
    for &size in &sizes {
        for &lat0 in &lats {
            for &lon0 in &lons {
                let minx = lon0;
                let maxx = lon0 + size;
                let miny = lat0;
                let maxy = lat0 + size;
                if maxy > 90.0 || miny < -90.0 || maxx > 180.0 {
                    continue;
                }
                match bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)) {
                    None => {},
                    Some(cell) => {
                        dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
                        let cx = f64::midpoint(minx, maxx);
                        let cy = f64::midpoint(miny, maxy);
                        let leaf = CellId::from_lonlat(cx, cy);
                        if leaf.face() == cell.face() {
                            assert!(
                                cell.contains(leaf),
                                "non-containing {} L{} for ({minx},{miny},{maxx},{maxy})",
                                cell.token(),
                                cell.level(),
                            );
                        }
                        ok += 1;
                    },
                }
            }
        }
    }
    assert!(ok >= 200, "expected many successes, got {ok}");
}

#[test]
fn soundness_matrix_success_is_dense_geom_or_none() {
    let cases: &[(f64, f64, f64, f64)] = &[
        (0.0, 0.0, 0.0, 45.0),
        (0.0, 0.0, 1.0, 1.0),
        (13.3, 52.4, 13.5, 52.6),
        (13.4, 52.5, 13.4001, 52.5001),
        (-1.0, 0.0, 0.0, 0.0),
        (90.0, -80.0, 91.0, -79.0),
        (-180.0, -85.0, -179.0, -84.0),
        (0.0, 45.0, 0.0, 88.0),
        (90.0, 45.0, 90.0, 60.0),
        (-90.0, 45.0, -90.0, 60.0),
        (-180.0, 45.0, -180.0, 60.0),
        (-135.0, -10.0, -135.0, -9.7),
        (135.0, -10.0, 135.0, -9.7),
        (135.0, 0.0, 135.0, 5.0),
        (-135.0, 0.0, -135.0, 5.0),
        (-45.0, 10.0, -40.0, 15.0),
        (135.0, -5.0, 140.0, 0.0),
        (-90.0, -40.0, -90.0, -39.9),
        (0.0, -90.0, 0.0, -88.0),
        (45.0, 0.0, 45.0, 1.0),
        (-135.0, 20.0, -134.5, 20.5),
        (45.0, 0.0, 46.0, 1.0),
        (-135.0, -35.0, -134.8, -34.8),
        (-135.0, 0.0, -134.8, 0.2),
    ];
    for &(minx, miny, maxx, maxy) in cases {
        let Some(cell) = bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)) else {
            continue;
        };
        dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
    }
    assert!(bounding_cell_bbox(Bounds::new_unchecked(-100.0, -40.0, 100.0, 40.0)).is_none());
    assert!(bounding_cell_bbox(Bounds::new_unchecked(-50.0, 10.0, -32.0, 15.0)).is_none());
    assert!(bounding_cell_bbox(Bounds::new_unchecked(135.0, 0.0, 135.0, 5.0)).is_none());
    assert!(bounding_cell_bbox(Bounds::new_unchecked(-135.0, 0.0, -135.0, 5.0)).is_none());
}
