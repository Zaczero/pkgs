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

fn assert_ring_rows(rows: impl IntoIterator<Item = RingRow>, reversed: bool, shift: f64) {
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
        let baseline = signature(&shape(&row.rings, row.multi));
        assert_expected(&row, &baseline);
        for offset in 0..variants {
            let actual = signature(&transformed(&row, offset, reversed, shift));
            if actual != baseline {
                failures.push(format!(
                    "{} rot{offset} reverse={reversed} shift={shift}: {actual:?} != {baseline:?}",
                    row.name
                ));
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

macro_rules! ring_rule_group {
    ($name:ident, $group:expr) => {
        mod $name {
            use super::*;

            #[test]
            fn forward() {
                assert_ring_rows(
                    rows().into_iter().filter(|row| ring_group(row) == $group),
                    false,
                    0.0,
                );
            }

            #[test]
            fn forward_east_period() {
                assert_ring_rows(
                    rows().into_iter().filter(|row| ring_group(row) == $group),
                    false,
                    360.0,
                );
            }

            #[test]
            fn forward_west_period() {
                assert_ring_rows(
                    rows().into_iter().filter(|row| ring_group(row) == $group),
                    false,
                    -360.0,
                );
            }

            #[test]
            fn reversed() {
                assert_ring_rows(
                    rows().into_iter().filter(|row| ring_group(row) == $group),
                    true,
                    0.0,
                );
            }

            #[test]
            fn reversed_east_period() {
                assert_ring_rows(
                    rows().into_iter().filter(|row| ring_group(row) == $group),
                    true,
                    360.0,
                );
            }

            #[test]
            fn reversed_west_period() {
                assert_ring_rows(
                    rows().into_iter().filter(|row| ring_group(row) == $group),
                    true,
                    -360.0,
                );
            }
        }
    };
}

ring_rule_group!(north_cap_ordinary_cut, 0);
ring_rule_group!(north_cap_seam_cut, 1);
ring_rule_group!(south_cap_ordinary_cut, 2);
ring_rule_group!(south_cap_seam_cut, 3);
ring_rule_group!(full_longitude_band, 4);
ring_rule_group!(pole_and_period_contact, 5);
ring_rule_group!(ordinary_and_cross_ring, 6);
