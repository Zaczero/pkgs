use std::ops::ControlFlow;

use super::super::events::{ContactPoint, compare_cut_events, exact_piece_contacts};
use super::super::{
    CutEvent, ExactChartPoint, ExactQuotientKey, FallibleAvl, compare_quotient_keys, quotient_key,
};
use super::support::{degrees, inventory, line, ring};
use crate::error::ErrorKind;
use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    ExpansionBudget, GeometryErrorKind, Polygon, Shape, for_each_candidate_pair,
};
use crate::work::Unmetered;

#[test]
fn pair_contacts_preserve_endpoint_ownership_and_each_exact_parameter() {
    for (name, left, right, expected) in [
        (
            "endpoint touch",
            [(0.0, 0.0), (2.0, 0.0)],
            [(2.0, 0.0), (2.0, 3.0)],
            vec![(2.0, 0.0, 1.0, 0.0)],
        ),
        (
            "endpoint on interior",
            [(0.0, 0.0), (4.0, 0.0)],
            [(2.0, 0.0), (2.0, 3.0)],
            vec![(2.0, 0.0, 0.5, 0.0)],
        ),
        (
            "collinear endpoint touch",
            [(0.0, 0.0), (2.0, 0.0)],
            [(2.0, 0.0), (4.0, 0.0)],
            vec![(2.0, 0.0, 1.0, 0.0)],
        ),
        (
            "partial overlap",
            [(0.0, 0.0), (4.0, 0.0)],
            [(2.0, 0.0), (6.0, 0.0)],
            vec![(4.0, 0.0, 1.0, 0.5), (2.0, 0.0, 0.5, 0.0)],
        ),
        (
            "coincident reversed",
            [(0.0, 0.0), (4.0, 0.0)],
            [(4.0, 0.0), (0.0, 0.0)],
            vec![(0.0, 0.0, 0.0, 1.0), (4.0, 0.0, 1.0, 0.0)],
        ),
        (
            "coincident forward",
            [(0.0, 0.0), (4.0, 0.0)],
            [(0.0, 0.0), (4.0, 0.0)],
            vec![(0.0, 0.0, 0.0, 0.0), (4.0, 0.0, 1.0, 1.0)],
        ),
    ] {
        let sources = inventory(degrees(), &[line(&left), line(&right)]);
        let mut actual = Vec::new();
        exact_piece_contacts(
            &sources.pieces[0],
            &sources.pieces[1],
            &mut Unmetered,
            |point, left, right, _work| {
                let ContactPoint::Endpoint(point) = point else {
                    panic!("{name}: a source endpoint was reconstructed");
                };
                actual.push((point.clone(), left, right));
                Ok(())
            },
        )
        .expect("exact pair contacts");
        assert_eq!(actual.len(), expected.len(), "{name}");
        for ((point, left, right), (x, y, left_t, right_t)) in actual.iter().zip(expected) {
            let expected = ExactChartPoint::from_rationals(
                ExactRational::from_f64(x),
                ExactRational::from_f64(y),
            );
            assert!(point.same_position(&expected).unwrap(), "{name}");
            assert!(
                left.try_cmp(&ExactRational::from_f64(left_t))
                    .unwrap()
                    .is_eq()
            );
            assert!(
                right
                    .try_cmp(&ExactRational::from_f64(right_t))
                    .unwrap()
                    .is_eq()
            );
        }
    }
}

#[test]
fn interior_crossing_remains_constructed_and_refuses_new_node_expansion() {
    let sources = inventory(degrees(), &[
        line(&[(0.0, 0.0), (3.0, 3.0)]),
        line(&[(0.0, 2.0), (3.0, 0.0)]),
    ]);
    let mut contacts = 0;
    exact_piece_contacts(
        &sources.pieces[0],
        &sources.pieces[1],
        &mut Unmetered,
        |point, left, right, _work| {
            let ContactPoint::Constructed(point) = point else {
                panic!("a proper rational crossing is not a seeded endpoint");
            };
            let fifth = ExactRational::from_f64(5.0);
            let coordinate = ExactRational::from_f64(6.0).try_quotient(&fifth).unwrap();
            let parameter = ExactRational::from_f64(2.0).try_quotient(&fifth).unwrap();
            assert!(point.x.try_cmp(&coordinate).unwrap().is_eq());
            assert!(point.y.try_cmp(&coordinate).unwrap().is_eq());
            assert!(left.try_cmp(&parameter).unwrap().is_eq());
            assert!(right.try_cmp(&parameter).unwrap().is_eq());
            contacts += 1;
            Ok(())
        },
    )
    .expect("proper crossing contacts");
    assert_eq!(contacts, 1);

    let mut budget = ExpansionBudget::new("contact_test", "exact chart");
    budget
        .add(GENERATED_ITEM_LIMIT)
        .expect("full generated allowance");
    let error = sources
        .node(&mut budget)
        .expect_err("a new node must be charged");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge {
            operation: "contact_test",
            ..
        })
    ));
}

#[test]
fn pair_refinement_refuses_primitive_work_before_emitting_a_contact() {
    let sources = inventory(degrees(), &[
        line(&[(0.0, 0.0), (2.0, 0.0)]),
        line(&[(2.0, 0.0), (2.0, 3.0)]),
    ]);
    let mut work = EvaluationBudget::with_remaining_ticks_for_test("contact_test", 0, 0)
        .expect("exhausted primitive allowance");
    let mut contacts = 0;
    let error = exact_piece_contacts(
        &sources.pieces[0],
        &sources.pieces[1],
        &mut work,
        |_, _, _, _| {
            contacts += 1;
            Ok(())
        },
    )
    .expect_err("exact contact work cannot bypass its supplied account");
    assert_eq!(contacts, 0);
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "contact_test",
            ..
        })
    ));
}

#[test]
fn coincident_contacts_keep_each_source_occurrence_across_seam_aliases() {
    for (longitude, alias) in [(0.0, 0.0), (180.0, -180.0)] {
        let sources = inventory(degrees(), &[
            line(&[(longitude, 0.0), (longitude, 4.0)]),
            line(&[(alias, 4.0), (alias, 0.0)]),
            line(&[(longitude, 0.0), (longitude, 4.0)]),
        ]);
        let mut budget = ExpansionBudget::new("contact_test", "exact chart");
        let chart = sources.node(&mut budget).expect("coincident source noding");
        assert_eq!(chart.nodes.len(), 2);
        let mut owners = chart
            .atoms
            .iter()
            .flat_map(|atom| &atom.owners)
            .map(|owner| {
                assert!(owner.source_range.start.is_zero());
                assert!(owner.source_range.end.is_one());
                owner.source_edge.0
            })
            .collect::<Vec<_>>();
        owners.sort_unstable();
        assert_eq!(owners, [0, 1, 2]);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn grid_contacts_reuse_seeded_endpoints_without_constructed_key_search() {
    let shapes = (0..16)
        .flat_map(|y| {
            (0..32).map(move |x| {
                let x = f64::from(x);
                let y = f64::from(y);
                Shape::Polygon(Polygon::new(
                    ring(&[
                        (x, y),
                        (x + 1.0, y),
                        (x + 1.0, y + 1.0),
                        (x, y + 1.0),
                        (x, y),
                    ]),
                    vec![],
                ))
            })
        })
        .collect::<Vec<_>>();
    let sources = inventory(degrees(), &shapes);
    let mut cuts = FallibleAvl::with_capacity(4_096);
    let mut nodes = FallibleAvl::with_capacity(561);
    let mut work = Unmetered;
    let mut compare_cuts =
        |left: &CutEvent, right: &CutEvent, _: &mut Unmetered| compare_cut_events(left, right);
    for piece in &sources.pieces {
        for (parameter, point) in [
            (ExactRational::zero(), &piece.start),
            (ExactRational::one(), &piece.end),
        ] {
            cuts.insert_with_work(
                CutEvent {
                    piece: piece.id,
                    parameter,
                    point: point.clone(),
                },
                &mut work,
                &mut compare_cuts,
            )
            .unwrap();
            nodes
                .insert(
                    quotient_key(sources.frame, point).unwrap(),
                    &mut compare_quotient_keys,
                )
                .unwrap();
        }
    }
    let mut comparisons = 0;
    let mut compare_keys =
        |left: &ExactQuotientKey, right: &ExactQuotientKey, _: &mut Unmetered| {
            comparisons += 1;
            compare_quotient_keys(left, right)
        };
    let mut budget = ExpansionBudget::new("contact_test", "exact chart");
    let proxies = sources
        .pieces
        .iter()
        .map(|piece| piece.outward_proxy().unwrap().unwrap())
        .collect::<Vec<_>>();
    let mut pairs = 0;
    let mut contacts = 0;
    let result = for_each_candidate_pair(
        &proxies,
        |index| u32::try_from(index).unwrap(),
        |left, right| {
            pairs += 1;
            exact_piece_contacts(
                &sources.pieces[left],
                &sources.pieces[right],
                &mut work,
                |point, _, _, _| {
                    assert!(matches!(point, ContactPoint::Endpoint(_)));
                    contacts += 1;
                    Ok(())
                },
            )
            .unwrap();
            sources
                .refine_piece_pair(
                    [left, right],
                    None,
                    &mut cuts,
                    &mut nodes,
                    &mut compare_cuts,
                    &mut compare_keys,
                    &mut budget,
                    &mut work,
                )
                .unwrap();
            ControlFlow::<()>::Continue(())
        },
    )
    .unwrap();
    assert_eq!(result, ControlFlow::Continue(()));
    assert!(pairs > 0);
    assert!(contacts > 0);
    assert_eq!(
        comparisons, 0,
        "all grid contact keys were seeded before pair refinement"
    );
    assert_eq!(budget.used(), 0);
    eprintln!("grid pairs={pairs} contacts={contacts} constructed_key_comparisons={comparisons}");
    let mut whole_budget = ExpansionBudget::new("contact_test", "exact chart");
    let chart = sources.node(&mut whole_budget).expect("whole grid nodes");
    assert_eq!(chart.nodes.len(), 561);
    assert_eq!(chart.atoms.len(), 1_072);
    assert_eq!(
        chart
            .atoms
            .iter()
            .map(|atom| atom.owners.len())
            .sum::<usize>(),
        2_048
    );
    assert_eq!(whole_budget.used(), 0);
}
