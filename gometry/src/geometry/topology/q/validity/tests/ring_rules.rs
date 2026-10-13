use super::support::{Expectation, RingRow, Signature, rows, shape, signature};
use crate::geometry::Shape;

fn rotate(ring: &[(f64, f64)], offset: usize) -> Vec<(f64, f64)> {
    let (_, open) = ring
        .split_last()
        .expect("the ring corpus stores an explicit closure");
    if open.is_empty() {
        return ring.to_vec();
    }
    let offset = offset.checked_rem(open.len()).unwrap_or(0);
    let mut rotated = open
        .iter()
        .cycle()
        .skip(offset)
        .take(open.len())
        .copied()
        .collect::<Vec<_>>();
    rotated.push(
        *rotated
            .first()
            .expect("rotating a nonempty open ring keeps one point"),
    );
    rotated
}

fn transformed(row: &RingRow, offset: usize, reversed: bool, shift: f64) -> Shape {
    let rings = row
        .rings
        .iter()
        .enumerate()
        .map(|(index, ring)| {
            let mut points = if index == 0 {
                rotate(ring, offset)
            } else {
                ring.clone()
            };
            if reversed {
                let _ = points
                    .pop()
                    .expect("the ring corpus stores an explicit closure");
                points.reverse();
                let first = *points
                    .first()
                    .expect("reversing a nonempty open ring keeps one point");
                points.push(first);
            }
            points
                .into_iter()
                .map(|(x, y)| (x + shift, y))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    shape(&rings, row.multi)
}

fn assert_expected(row: &RingRow, signature: &Signature) {
    match row.expectation {
        Expectation::Valid => assert_eq!(signature.code, None, "{}", row.name),
        Expectation::Invalid(code) => {
            assert_eq!(signature.code, Some(code), "{}", row.name);
        },
    }
}

fn assert_ring_rows(rows: impl IntoIterator<Item = RingRow>) {
    let mut failures = Vec::new();
    for row in rows {
        let closed_ring = row
            .rings
            .first()
            .expect("every rule row contains one source ring");
        let variants = closed_ring
            .len()
            .checked_sub(1)
            .expect("every source ring stores a closure");
        let mut baseline = None;
        for shift in [0.0, 360.0, -360.0] {
            for reversed in [false, true] {
                for offset in 0..variants {
                    let actual = signature(&transformed(&row, offset, reversed, shift));
                    if let Some(expected) = &baseline {
                        if &actual != expected {
                            failures.push(format!(
                                    "{} rot{offset} reverse={reversed} shift={shift}: {actual:?} != {expected:?}",
                                    row.name
                                ));
                        }
                    } else {
                        assert_expected(&row, &actual);
                        baseline = Some(actual);
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "spelling-invariant rows failed: {failures:#?}"
    );
}

fn ring_group(row: &RingRow) -> u8 {
    let first_longitudes = ["@0 ", "@40 ", "@-90 ", "@100 "];
    let first_cap = row.name.starts_with("U1 pair")
        || first_longitudes
            .iter()
            .any(|longitude| row.name.contains(longitude));
    if row.name.contains("lat80") {
        u8::from(!first_cap)
    } else if row.name.contains("lat-80") {
        if first_cap { 2 } else { 3 }
    } else if row.name.starts_with("band") {
        4
    } else if row.periodic {
        5
    } else {
        6
    }
}

#[test]
fn source_prototype_contains_84_ring_rule_rows() {
    assert_eq!(rows().len(), 84);
}

#[test]
fn north_cap_ordinary_cut_rules_are_invariant_under_period_lifts() {
    assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 0));
}

#[test]
fn north_cap_seam_cut_rules_are_invariant_under_period_lifts() {
    assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 1));
}

#[test]
fn south_cap_ordinary_cut_rules_are_invariant_under_period_lifts() {
    assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 2));
}

#[test]
fn south_cap_seam_cut_rules_are_invariant_under_period_lifts() {
    assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 3));
}

#[test]
fn full_longitude_band_rules_are_invariant_under_period_lifts() {
    assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 4));
}

#[test]
fn pole_and_period_contact_rules_are_invariant_under_period_lifts() {
    assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 5));
}

#[test]
fn ordinary_and_cross_ring_facts_are_invariant_under_rotation() {
    assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 6));
}
