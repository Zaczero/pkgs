//! Symbolic ties and deterministic site insertion ordering.

use super::MeshCoordinates;
use crate::error::Result;
use crate::geometry::numerics::product::ExactSign;

/// Lexicographically ordered infinitesimal vertical lifts of the paraboloid.
/// On a zero determinant, its first nonzero lift cofactor selects one diagonal;
/// the exact geometric predicate remains unchanged for circumcenter certificates.
pub(super) fn symbolic_incircle_tie<C: MeshCoordinates>(
    coordinates: &C,
    [a, b, c, d]: [C::Query; 4],
) -> Result<ExactSign> {
    let mut sites = [(a, 0), (b, 1), (c, 2), (d, 3)];
    sites.sort_unstable_by(|&(left, _), &(right, _)| coordinates.compare(left, right));
    for (_, row) in sites {
        let (sign, negate) = match row {
            0 => (coordinates.orient([b, c, d])?, false),
            1 => (coordinates.orient([a, c, d])?, true),
            2 => (coordinates.orient([a, b, d])?, false),
            _ => (coordinates.orient([a, b, c])?, true),
        };
        if sign != ExactSign::Zero {
            return Ok(if negate {
                match sign {
                    ExactSign::Positive => ExactSign::Negative,
                    ExactSign::Negative => ExactSign::Positive,
                    ExactSign::Zero => ExactSign::Zero,
                }
            } else {
                sign
            });
        }
    }
    Ok(ExactSign::Zero)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(super) enum InsertionOrder {
    Original,
    Brio,
    Reversed,
}

#[cfg(test)]
thread_local! {
    pub(super) static INSERTION_ORDER: std::cell::Cell<InsertionOrder> =
        const { std::cell::Cell::new(InsertionOrder::Brio) };
}
pub(super) fn order_interior(interior: &mut [(u32, usize)]) {
    #[cfg(test)]
    match INSERTION_ORDER.with(std::cell::Cell::get) {
        InsertionOrder::Original => {
            interior.sort_unstable();
            return;
        },
        InsertionOrder::Reversed => {
            interior.sort_unstable();
            interior.reverse();
            return;
        },
        InsertionOrder::Brio => {},
    }
    interior.sort_unstable_by_key(|&(hilbert, site)| {
        let mut sample = (site as u64).wrapping_add(264);
        sample = (sample ^ (sample >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        sample = (sample ^ (sample >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        sample ^= sample >> 31;
        (std::cmp::Reverse(sample.leading_zeros()), hilbert, site)
    });
}
