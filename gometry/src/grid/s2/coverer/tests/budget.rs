use super::*;

/// Fixed-level covering of a box: complete (every interior sample's
/// ancestor cell is in `outer`), exact (no stray cells), and
/// `interior ⊆ outer` with covered centers.
#[test]
fn fixed_level_box_covering_is_complete_and_exact() {
    let source = box_shape(13.0, 52.0, 14.0, 53.0);
    let covering = fixed(10)
        .cover_identity(&source)
        .expect("small fixed-level covering is within budget");
    let outer = covering.outer();
    let interior = covering.interior();
    assert!(!outer.is_empty());
    assert!(!interior.is_empty());
    assert!(outer.array_windows::<2>().all(|[a, b]| a < b));
    // Completeness: sampled interior points classify into outer cells.
    for i in 0..=20 {
        for j in 0..=20 {
            let lon = 13.0 + f64::from(i) / 20.0;
            let lat = 52.0 + f64::from(j) / 20.0;
            let leaf = CellId::from_lonlat(lon, lat).parent(10).expect("coarser");
            assert!(has(&outer, leaf), "missing cell for lon={lon} lat={lat}");
        }
    }
    // Interior certificates: contained in outer, centers covered.
    for &id in &interior {
        assert!(has(&outer, id));
        let center = Cell::from_id(id).center_lonlat();
        assert!(source.covers_point(center));
    }
    // The exact covering is materially tighter than the bbox: cells
    // far outside the box never appear.
    for &id in &outer {
        let center = Cell::from_id(id).center_lonlat();
        assert!(
            (12.7..=14.3).contains(&center.x) && (51.7..=53.3).contains(&center.y),
            "stray cell at {center:?}"
        );
    }
}

/// An L-shaped (concave) source: the exact coverer excludes the
/// notch — the rectangle coverer's documented false positives.
#[test]
fn concave_source_excludes_notch() {
    let shell = vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(4.0, 0.0),
        Point::new_raw_xy(4.0, 1.0),
        Point::new_raw_xy(1.0, 1.0),
        Point::new_raw_xy(1.0, 4.0),
        Point::new_raw_xy(0.0, 4.0),
        Point::new_raw_xy(0.0, 0.0),
    ];
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&shell).unwrap()).unwrap(),
        Vec::new(),
    ));
    let covering = fixed(8)
        .cover_identity(&source)
        .expect("small fixed-level covering is within budget");
    let outer = covering.outer();
    // The notch interior (around 3, 3) is far from the L; its cell must
    // not appear, while the bbox covering would include it.
    let notch = CellId::from_lonlat(3.0, 3.0).parent(8).expect("coarser");
    assert!(!has(&outer, notch));
    let arm = CellId::from_lonlat(0.5, 3.5).parent(8).expect("coarser");
    assert!(has(&outer, arm));
}

/// Adaptive covers use `target_cells` as a refinement target, while finite
/// `max_cells` remains a hard cap during construction.  With `min_level=0`,
/// this reaches phase 2 directly from the face queue; the cap decision is in
/// the adaptive staged-refinement decision, not forced descent.
#[test]
fn adaptive_max_cells_is_a_hard_cap() {
    let source = box_shape(40.0, -10.0, 50.0, 10.0);
    let unlimited = Coverer {
        min_level: 0,
        max_level: 4,
        level_mod: 1,
        max_cells: None,
        target_cells: 64,
    }
    .cover_identity(&source)
    .expect("unlimited adaptive cover");
    assert!(
        unlimited.outer().iter().any(|id| id.level() > 0),
        "fixture must enter adaptive refinement"
    );
    let coverer = Coverer {
        min_level: 0,
        max_level: 4,
        level_mod: 1,
        max_cells: Some(2),
        target_cells: 64,
    };
    // Phase 2 stages each root's children, then line 311 refuses that
    // refinement growth because it would exceed the finite cap.  The
    // conforming level-0 parents remain within the cap and are emitted.
    let capped_small = coverer
        .cover_identity(&source)
        .expect("adaptive cap must refuse queue growth without exceeding it");
    assert!(capped_small.outer().iter().all(|id| id.level() == 0));
    assert_ne!(capped_small.outer(), unlimited.outer());

    let capped = Coverer {
        min_level: 0,
        max_level: 4,
        level_mod: 1,
        max_cells: Some(unlimited.outer().len()),
        target_cells: 64,
    }
    .cover_identity(&source)
    .expect("finite adaptive budget matching the output must fit");
    assert_eq!(capped.outer(), unlimited.outer());
}

/// Seam-straddling sources stay narrow (no degradation to a global
/// covering) and cover both sides of the seam.
#[test]
fn seam_source_stays_narrow() {
    let east = box_shape(179.5, -1.0, 180.0, 1.0);
    let west = box_shape(-180.0, -1.0, -179.5, 1.0);
    let source = match (east, west) {
        (Shape::Polygon(a), Shape::Polygon(b)) => Shape::MultiPolygon(vec![a, b].into()),
        _ => unreachable!(),
    };
    let covering = fixed(8)
        .cover_identity(&source)
        .expect("small fixed-level covering is within budget");
    let outer = covering.outer();
    assert!(!outer.is_empty());
    // Both spellings are represented.
    let east_cell = CellId::from_lonlat(179.9, 0.0).parent(8).expect("coarser");
    let west_cell = CellId::from_lonlat(-179.9, 0.0).parent(8).expect("coarser");
    assert!(has(&outer, east_cell));
    assert!(has(&outer, west_cell));
    // Narrow: nothing near lon 0, and far from global scale.
    let far = CellId::from_lonlat(0.0, 0.0).parent(8).expect("coarser");
    assert!(!has(&outer, far));
    assert!(outer.len() < 2000, "{} cells", outer.len());
}

/// Point and line sources produce boundary-only coverings (no false
/// interior certificates on measure-zero geometry).
#[test]
fn thin_sources_have_no_interior() {
    let point = Shape::Point(Point::new_raw_xy(13.4, 52.5));
    let covering = fixed(12)
        .cover_identity(&point)
        .expect("small fixed-level covering is within budget");
    let outer = covering.outer();
    assert_eq!(outer.len(), 1);
    assert!(covering.interior().is_empty());
    let leaf = CellId::from_lonlat(13.4, 52.5).parent(12).expect("coarser");
    assert_eq!(outer, vec![leaf]);
}

/// Polar-face kite cells (the refuted expanded-proxy counterexample
/// region): interior certificates near lat 85 stay sound — every
/// certified cell's vertices, center, AND edge midpoints lie inside
/// the source.
#[test]
fn polar_kite_interior_certificates_are_sound() {
    let source = box_shape(20.0, 80.0, 70.0, 88.0);
    let covering = fixed(6)
        .cover_identity(&source)
        .expect("small fixed-level covering is within budget");
    for id in covering.interior() {
        let cell = Cell::from_id(id);
        assert!(source.covers_point(cell.center_lonlat()), "{id:?} center");
        for vertex in cell.vertices_lonlat() {
            assert!(source.covers_point(vertex), "{id:?} vertex {vertex:?}");
        }
        // Edge midpoints are also exact cell points (children's shared
        // corners): sample via the four children's vertices.
        if let Some(children) = id.children() {
            for child in children {
                for vertex in Cell::from_id(child).vertices_lonlat() {
                    assert!(
                        source.covers_point(vertex),
                        "{id:?} child vertex {vertex:?}"
                    );
                }
            }
        }
    }
}

/// Fixed-level coverings never emit below `min_level`, even when an
/// explicit soft/hard `max_cells` is present but large enough to fit the
/// true fixed-level cover (F7: soft-budget must not substitute coarser
/// interior cells).
#[test]
fn fixed_level_soft_budget_never_emits_below_min_level() {
    let source = box_shape(0.0, 0.0, 1.0, 1.0);
    let unlimited = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    }
    .cover_identity(&source)
    .expect("unlimited fixed-level covering");
    let fit = unlimited.outer().len();
    assert!(fit > 0);
    // Finite budget strictly above the fit count must match unlimited
    // (no silent coarsening of interior cells below min_level).
    let capped = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: Some(fit + 32),
        target_cells: 8,
    }
    .cover_identity(&source)
    .expect("fitting fixed-level covering under soft budget");
    assert_eq!(capped.outer(), unlimited.outer());
    assert!(
        capped.outer().iter().all(|id| id.level() == 10),
        "fixed-level cover emitted non-level-10 cells"
    );
}

/// A large areal source forced down to a fine `min_level` expands past the
/// cell budget; the coverer fails deterministically (naming `max_cells`)
/// during the interior descendant emission, never flooding memory.
#[test]
fn cover_rejects_fine_min_level_before_flooding() {
    let source = box_shape(-60.0, -40.0, 60.0, 40.0);
    let coverer = Coverer {
        min_level: 16,
        max_level: 16,
        level_mod: 1,
        max_cells: Some(usize::try_from(crate::grid::GRID_MAX_CELLS).unwrap()),
        target_cells: 8,
    };
    let err = coverer
        .cover_identity(&source)
        .expect_err("world-scale fixed min_level exceeds the cell budget");
    assert!(matches!(
        err,
        crate::grid::CoverError::Budget(error)
            if error.limit == usize::try_from(crate::grid::GRID_MAX_CELLS).unwrap()
    ));
    assert!(err.to_string().contains("max_cells"));
}

/// Revert-sensitive: a short line whose unlimited cover is K cells must
/// succeed for every `max_cells >= K` with the same tokens, and raise only
/// for `max_cells < K`. Transient fail-open DFS frontier size must not be
/// charged against the emission budget (the max_cells=1 repro).
#[test]
fn fixed_level_budget_matches_unlimited_threshold() {
    let source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-75.0, 40.0),
                Point::new_raw_xy(-74.99, 40.01),
            ])
            .unwrap(),
        )
        .expect("test line is valid"),
    );
    let unlimited = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    }
    .cover_identity(&source)
    .expect("unlimited short-line covering");
    let k = unlimited.outer().len();
    assert_eq!(k, 1, "repro line must fit in one L10 cell");
    assert_eq!(unlimited.stats().visible_queue_peak, 0);
    assert_eq!(unlimited.outer()[0].token(), "89c6b5");

    for m in 1..=k + 2 {
        let got = Coverer {
            min_level: 10,
            max_level: 10,
            level_mod: 1,
            max_cells: Some(m),
            target_cells: 8,
        }
        .cover_identity(&source)
        .unwrap_or_else(|_| panic!("budget {m} must fit K={k} cover"));
        assert_eq!(got.outer(), unlimited.outer(), "budget {m}");
    }
    // K-1 must raise when K > 0 (here K=1 ⇒ no smaller positive budget to
    // probe; use a multi-cell line for the raise side).
    let longer = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-75.0, 40.0),
                Point::new_raw_xy(-74.0, 41.0),
            ])
            .unwrap(),
        )
        .expect("longer line"),
    );
    let long_u = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    }
    .cover_identity(&longer)
    .expect("unlimited longer line");
    let n = long_u.outer().len();
    assert!(n > 1, "longer line should need multiple L10 cells, got {n}");
    let fit = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: Some(n),
        target_cells: 8,
    }
    .cover_identity(&longer)
    .expect("budget N must fit");
    assert_eq!(fit.outer(), long_u.outer());
    assert_eq!(fit.cells.as_slice(), long_u.cells.as_slice());
    let above_budget = n.checked_add(1).expect("test budget fits usize");
    let above = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: Some(above_budget),
        target_cells: 8,
    }
    .cover_identity(&longer)
    .expect("budget N+1 must fit");
    assert_eq!(above.cells.as_slice(), long_u.cells.as_slice());
    let err = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: Some(n - 1),
        target_cells: 8,
    }
    .cover_identity(&longer)
    .expect_err("budget N-1 must raise");
    assert!(matches!(
        err,
        crate::grid::CoverError::Budget(error) if error.limit == n - 1
    ));
    assert!(err.to_string().contains("max_cells"));
}
