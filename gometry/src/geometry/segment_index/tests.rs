use std::ops::ControlFlow;

use super::{CHAIN_MIN_SEGMENTS, RUN_NODING_MIN, for_each_candidate_pair, single_chain};
use crate::geometry::{Segment, XY, segments_intersect};

#[cfg(test)]
mod sweep_characterization {
    //! Golden contract for the candidate-pair sweep: it visits a SUPERSET of
    //! every truly-intersecting pair (no real interaction is ever dropped) and
    //! never visits an unordered pair twice — at every size band and both
    //! thresholds. The exact kernels are the final filter, so the sweep's only
    //! correctness duty is completeness; this test locks it so any later change
    //! to the sweep's input representation that loses a candidate fails loudly.
    use std::collections::BTreeSet;

    use super::*;

    fn structured_pool(n: usize) -> Vec<Segment> {
        let groups = n.saturating_sub(4).div_ceil(8);
        let xmax = (16 * groups + 12) as f64;
        let mut pool = vec![
            Segment {
                start: XY::new(0.0, 6.0),
                end: XY::new(xmax, 6.0),
            },
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(xmax, 12.0),
            },
            Segment {
                start: XY::new(0.0, 12.0),
                end: XY::new(xmax, 0.0),
            },
            Segment {
                start: XY::new(xmax / 2.0, -2.0),
                end: XY::new(xmax / 2.0, 14.0),
            },
        ];
        for group in 0..groups {
            let x = (16 * group) as f64;
            pool.extend([
                Segment {
                    start: XY::new(x, 0.0),
                    end: XY::new(x + 12.0, 12.0),
                },
                Segment {
                    start: XY::new(x, 12.0),
                    end: XY::new(x + 12.0, 0.0),
                },
                Segment {
                    start: XY::new(x - 2.0, 6.0),
                    end: XY::new(x + 14.0, 6.0),
                },
                Segment {
                    start: XY::new(x + 6.0, -2.0),
                    end: XY::new(x + 6.0, 14.0),
                },
                Segment {
                    start: XY::new(x + 3.0, 6.0),
                    end: XY::new(x + 9.0, 6.0),
                },
                Segment {
                    start: XY::new(x + 12.0, 12.0),
                    end: XY::new(x + 16.0, 12.0),
                },
                Segment {
                    start: XY::new(x, 1.0),
                    end: XY::new(x + 12.0, 13.0),
                },
                Segment {
                    start: XY::new(x, 18.0),
                    end: XY::new(x + 12.0, 18.0),
                },
            ]);
        }
        pool.truncate(n);
        pool
    }

    fn visited_pairs<const RUN_MIN: usize>(pool: &[Segment]) -> BTreeSet<(usize, usize)> {
        let mut seen = BTreeSet::new();
        let _ = for_each_candidate_pair::<RUN_MIN>(pool, single_chain, |a, b| {
            let key = if a < b { (a, b) } else { (b, a) };
            assert!(seen.insert(key), "pair {key:?} visited more than once");
            ControlFlow::Continue(())
        });
        seen
    }

    #[test]
    fn sweep_covers_every_intersecting_pair() {
        // Sizes spanning the brute (<6), flat-sweep, and run-sweep branches for
        // BOTH thresholds (RUN_NODING_MIN = 512, CHAIN_MIN_SEGMENTS = 1024).
        for &n in &[4_usize, 20, 200, 600, 1100, 2000] {
            let pool = structured_pool(n);
            let mut truly_intersecting: BTreeSet<(usize, usize)> = BTreeSet::new();
            for i in 0..n {
                for j in (i + 1)..n {
                    if segments_intersect(pool[i], pool[j]) {
                        truly_intersecting.insert((i, j));
                    }
                }
            }
            for visited in [
                visited_pairs::<RUN_NODING_MIN>(&pool),
                visited_pairs::<CHAIN_MIN_SEGMENTS>(&pool),
            ] {
                for &pair in &truly_intersecting {
                    assert!(
                        visited.contains(&pair),
                        "n={n}: intersecting pair {pair:?} dropped by the sweep",
                    );
                }
            }
        }
    }
}
