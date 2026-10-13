use super::super::candidate::Productivity;
use super::*;

#[test]
fn productive_state_is_isolated_between_sources() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-50.0, -40.0),
                Point::new_raw_xy(50.0, -40.0),
            ])
            .unwrap(),
        )
        .expect("fixture line is valid"),
    );
    let adaptive = Coverer {
        min_level: 0,
        max_level: MAX_LEVEL,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    };
    adaptive
        .cover_identity(&line)
        .expect("first source covers without a budget");

    let face = CellId::from_token("3").expect("canonical level-zero face token");
    let point = Shape::Point(Cell::from_id(face).center_lonlat());
    let covering = fixed(0)
        .cover_identity(&point)
        .expect("point has one level-zero owner");
    assert_eq!(covering.outer(), vec![face]);
}

#[test]
fn within_of_nonpolygonal_sources_returns_empty_without_classification() {
    let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-50.0, -40.0),
                Point::new_raw_xy(50.0, -40.0),
            ])
            .unwrap(),
        )
        .expect("fixture line is valid"),
    );
    let collection = Shape::geometry_collection(vec![point.clone(), line.clone()])
        .expect("nonpolygonal collection is valid");

    for source in [&point, &line, &collection] {
        for (min_level, max_level) in [(0, 9), (8, 8)] {
            for max_cells in [None, Some(1)] {
                let coverer = Coverer {
                    min_level,
                    max_level,
                    level_mod: 1,
                    max_cells,
                    target_cells: 8,
                };
                let covering = coverer
                    .cover_identity_for_rule(source, CoverRule::Within)
                    .expect("nonpolygonal within cover is empty and valid");
                let stats = covering.stats();

                assert!(covering.outer().is_empty());
                assert_eq!(stats.raw_classify_calls, 0, "{source:?} {coverer:?}");
                assert_eq!(stats.unique_local_point_probes, 0, "{source:?} {coverer:?}");
                assert_eq!(stats.committed_emissions, 0, "{source:?} {coverer:?}");
            }
        }
    }
}

#[test]
fn adaptive_productivity_reuses_nested_certificates_for_a_holed_polygon() {
    let source = polygon_with_hole();
    let target_cells = 8;
    let coverer = Coverer {
        min_level: 0,
        max_level: 8,
        level_mod: 1,
        max_cells: None,
        target_cells,
    };
    let covering = coverer
        .cover_identity_for_rule(&source, CoverRule::Within)
        .expect("holed polygon within cover");
    let outer = covering.outer();
    let stats = covering.stats();

    assert!(!outer.is_empty());
    assert!(
        covering.cells.iter().all(|(_, interior)| *interior),
        "within results carry the interior tag"
    );
    let oracle = RelateCoverContext {
        authority: GeographicCoverageSource::new(&source, SphericalGridTarget::S2)
            .expect("oracle source carrier allocates"),
        tester: PointBatchTester::new(&source).expect("oracle point tester allocates"),
        source: source.clone(),
        point_leaves: point_leaves(&source),
    };
    for id in outer {
        assert_eq!(
            classify_relate(&oracle, id),
            CellClass::Interior,
            "within result {} is not inside the independent relate oracle",
            id.token()
        );
    }
    assert!(stats.productive_path_reuses > 0, "{stats:?}");
    assert!(stats.productive_path_hits > 0, "{stats:?}");
    assert!(
        stats.carried_certificate_hits > stats.productive_path_reuses,
        "{stats:?}"
    );
    assert_eq!(
        stats.candidate_preparations, stats.raw_classify_calls,
        "carried certificates must bypass fresh candidate preparation: {stats:?}"
    );
    assert_eq!(stats.retained_map_entries, 0, "{stats:?}");
    assert!(stats.productive_frontier_peak <= target_cells, "{stats:?}");
    assert_eq!(
        stats.committed_emissions,
        covering.outer().len(),
        "{stats:?}"
    );
}

#[test]
fn fixed_within_emits_visible_terminals_without_staging_invisible_cells() {
    let source = polygon_with_hole();
    let coverer = fixed(8);
    let covering = coverer
        .cover_identity_for_rule(&source, CoverRule::Within)
        .expect("fixed within cover of a holed polygon");
    let stats = covering.stats();
    let expected = cover_with_relate_oracle(coverer, &source).interior();

    assert_eq!(covering.outer(), expected);
    assert!(!expected.is_empty());
    assert_eq!(stats.visible_queue_peak, 0, "{stats:?}");
    assert_eq!(stats.invisible_stack_peak, 0, "{stats:?}");
}

#[test]
fn complete_carried_fan_matches_fresh_certificates_without_repreparation() {
    let source = box_shape(-5.0, -5.0, 5.0, 5.0);
    let affine = GeographicCoverageSource::new(&source, SphericalGridTarget::S2)
        .expect("test source carrier allocates");
    let ctx = CoverContext::prepare(&source, &affine, WorkingShapeRelation::Identity)
        .expect("test cover context allocates");
    let coverer = Coverer {
        min_level: 0,
        max_level: 6,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    };

    let carried = Face::ALL.into_iter().find_map(|face| {
        let mut stats = CoverStats::default();
        let root =
            classify_one(&ctx, CellId::from_face(face), &mut stats).expect("face classification");
        let target = coverer.refinement_target(root.certificate.id.level())?;
        let productivity = productive_state(&ctx, root.certificate, coverer, &mut stats)
            .expect("productivity classification")?;
        let Productivity::Children(_) = &productivity else {
            return None;
        };
        let mut moved = Vec::new();
        stage_children_at(
            &ctx,
            root.certificate.id,
            target,
            Some(productivity),
            &mut moved,
            &mut stats,
        )
        .expect("carried fan moves");

        let mut fresh = Vec::new();
        let mut fresh_stats = CoverStats::default();
        stage_children_at(
            &ctx,
            root.certificate.id,
            target,
            None,
            &mut fresh,
            &mut fresh_stats,
        )
        .expect("fresh fan classifies");

        assert_eq!(
            moved
                .iter()
                .map(|candidate| candidate.certificate)
                .collect::<Vec<CandidateCertificate>>(),
            fresh
                .iter()
                .map(|candidate| candidate.certificate)
                .collect::<Vec<CandidateCertificate>>()
        );
        assert_eq!(
            stats.candidate_preparations, stats.raw_classify_calls,
            "{stats:?}"
        );
        assert_eq!(stats.carried_certificate_hits, moved.len(), "{stats:?}");
        assert_eq!(fresh_stats.candidate_preparations, fresh.len());
        Some(())
    });
    assert!(
        carried.is_some(),
        "fixture must expose a productive face fan"
    );
}

#[test]
fn adaptive_line_overlap_keeps_its_existing_traversal() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-50.0, -40.0),
                Point::new_raw_xy(50.0, -40.0),
            ])
            .unwrap(),
        )
        .expect("fixture line is valid"),
    );
    let overlap = Coverer {
        min_level: 0,
        max_level: 8,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    }
    .cover_identity(&line)
    .expect("line overlap cover");

    assert_eq!(
        overlap.stats().committed_emissions,
        overlap.outer().len(),
        "{:?}",
        overlap.stats()
    );
    assert!(
        overlap.stats().productive_frontier_peak <= 8,
        "{:?}",
        overlap.stats()
    );
    assert!(
        overlap.stats().visible_queue_peak > 0,
        "{:?}",
        overlap.stats()
    );
}

#[test]
fn nonpolygonal_center_rule_still_selects_line_cells() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-50.0, 0.0),
                Point::new_raw_xy(50.0, 0.0),
            ])
            .unwrap(),
        )
        .expect("equatorial fixture line is valid"),
    );
    let face = CellId::from_face(Face::F0);
    let coverer = Coverer {
        min_level: 0,
        max_level: 0,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    };

    let center = coverer
        .cover_identity_for_rule(&line, CoverRule::Center)
        .expect("line center cover");
    assert!(
        center.outer().contains(&face),
        "center omitted the equatorial face"
    );
}

#[test]
fn adaptive_level_mod_values_keep_emitted_levels_conforming() {
    let source = box_shape(10.0, 20.0, 12.0, 22.0);
    for level_mod in 1..=3 {
        let covering = Coverer {
            min_level: 2,
            max_level: 8,
            level_mod,
            max_cells: None,
            target_cells: 64,
        }
        .cover_identity(&source)
        .unwrap_or_else(|_| panic!("level_mod {level_mod}"));
        assert!(
            covering
                .outer()
                .iter()
                .all(|id| (id.level() - 2) % level_mod == 0),
            "level_mod {level_mod}: {:?}",
            covering.outer()
        );
    }
}

#[test]
fn rule_specific_sink_retains_only_visible_cells() {
    let source = box_shape(0.0, 0.0, 2.0, 2.0);
    let coverer = fixed(8);
    let overlap = coverer
        .cover_identity_for_rule(&source, CoverRule::Overlap)
        .expect("fixture overlap fits");
    let within = coverer
        .cover_identity_for_rule(&source, CoverRule::Within)
        .expect("fixture within fits");
    let center = coverer
        .cover_identity_for_rule(&source, CoverRule::Center)
        .expect("fixture center fits");
    let overlap_oracle = cover_with_relate_oracle(coverer, &source);

    let overlap_retained = overlap.outer().len();
    let within_retained = within.outer().len();
    let center_retained = center.outer().len();
    assert!(within_retained < overlap_retained);
    assert!(center_retained < overlap_retained);
    assert_eq!(overlap.cells.as_slice(), overlap_oracle.cells.as_slice());
    assert!(within.cells.iter().all(|(_, interior)| *interior));
}

#[test]
fn adaptive_target_ignores_unproductive_outside_frontier() {
    let source = box_shape(-75.0, 40.0, -74.9, 40.1);
    let detailed = Coverer {
        min_level: 1,
        max_level: 10,
        level_mod: 1,
        max_cells: None,
        target_cells: 1_000_000,
    }
    .cover_identity(&source)
    .expect("detailed adaptive cover");
    let targeted = Coverer {
        min_level: 1,
        max_level: 10,
        level_mod: 1,
        max_cells: None,
        target_cells: 4,
    }
    .cover_identity(&source)
    .expect("targeted adaptive cover");
    assert_eq!(detailed.outer().len(), 4);
    assert_eq!(targeted.outer(), detailed.outer());
    assert!(targeted.outer().iter().all(|id| id.level() == 10));

    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-75.0, 40.0),
                Point::new_raw_xy(-74.0, 40.5),
                Point::new_raw_xy(-73.0, 41.2),
            ])
            .unwrap(),
        )
        .expect("line is valid"),
    );
    let capped = Coverer {
        min_level: 1,
        max_level: 14,
        level_mod: 1,
        max_cells: Some(64),
        target_cells: 1_000_000,
    }
    .cover_identity(&line)
    .expect("capped adaptive cover");
    assert_eq!(capped.outer().len(), 64);
    assert_eq!(capped.stats().committed_emissions, 64);
    assert!(capped.stats().productive_frontier_peak <= 64);
    assert!(capped.outer().iter().all(|id| id.level() >= 1));
}

#[test]
fn adaptive_without_max_cells_remains_unlimited() {
    let source = box_shape(10.0, 40.0, 20.0, 50.0);
    let covering = Coverer {
        min_level: 4,
        max_level: 12,
        level_mod: 1,
        max_cells: None,
        target_cells: 64,
    }
    .cover_identity(&source)
    .expect("unlimited adaptive cover");
    let outer = covering.outer();
    assert!(outer.len() > 1);
    let levels: std::collections::BTreeSet<u8> = outer.iter().map(|id| id.level()).collect();
    assert!(levels.len() > 1, "expected mixed levels, got {levels:?}");
    assert!(levels.iter().all(|&level| (4..=12).contains(&level)));
}
