use super::support::{Signature, shape, signature};
use crate::geometry::Shape;

#[derive(Clone, Copy)]
enum Expectation {
    Valid,
    Invalid(&'static str),
}

struct RingRow {
    name: &'static str,
    rings: Vec<Vec<(f64, f64)>>,
    multi: bool,
    expectation: Expectation,
}

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

fn assert_ring_row(row: &RingRow, reversed: bool, shift: f64) {
    let mut failures = Vec::new();
    let closed_ring = row
        .rings
        .first()
        .expect("every rule row contains one source ring");
    let variants = closed_ring
        .len()
        .checked_sub(1)
        .expect("every source ring stores a closure");
    let baseline = signature(&shape(&row.rings, row.multi));
    assert_expected(row, &baseline);
    for offset in 0..variants {
        let actual = signature(&transformed(row, offset, reversed, shift));
        if actual != baseline {
            failures.push(format!(
                "{} rot{offset} reverse={reversed} shift={shift}: {actual:?} != {baseline:?}",
                row.name
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "spelling-invariant rows failed: {failures:#?}"
    );
}

/// Give each source row six independent spelling cases while retaining every rotation.
macro_rules! ring_rule_cases {
    ($(($name:ident, $label:literal, $rings:expr, $multi:expr, $expectation:expr)),* $(,)?) => {
        $(
            mod $name {
                use super::*;

                fn row() -> RingRow {
                    RingRow {
                        name: $label,
                        rings: $rings,
                        multi: $multi,
                        expectation: $expectation,
                    }
                }

                #[test]
                fn forward() { assert_ring_row(&row(), false, 0.0); }

                #[test]
                fn forward_east_period() { assert_ring_row(&row(), false, 360.0); }

                #[test]
                fn forward_west_period() { assert_ring_row(&row(), false, -360.0); }

                #[test]
                fn reversed() { assert_ring_row(&row(), true, 0.0); }

                #[test]
                fn reversed_east_period() { assert_ring_row(&row(), true, 360.0); }

                #[test]
                fn reversed_west_period() { assert_ring_row(&row(), true, -360.0); }
            }
        )*
    };
}

mod cases;
