use std::ops::ControlFlow;

use crate::geometry::segment_index::{
    for_each_candidate_pair, for_each_indexed_overlapping_bounds_pair, single_chain,
};
use crate::geometry::{Bounds, Segment, XY, segments_intersect};

#[cfg(test)]
mod sweep_characterization {
    //! Golden contract for the candidate-pair sweep: it visits a SUPERSET of
    //! every truly-intersecting pair (no real interaction is ever dropped) and
    //! never visits an unordered pair twice — at every size band and both
    //! thresholds. The exact kernels are the final filter, so the sweep's only
    //! correctness duty is completeness; this test locks it so any later change
    //! to the sweep's input representation that loses a candidate fails loudly.
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

    /// One unordered pair packed into a single sortable key.  The pair sets
    /// here reach the low millions, where a sorted `Vec` of packed keys costs
    /// a fraction of a `BTreeSet` of tuples in both allocation and probe time.
    fn pair_key(a: usize, b: usize) -> u64 {
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        ((lo as u64) << 32) | hi as u64
    }

    /// The visited set, with the never-visited-twice half of the contract
    /// checked in place. Sorting makes a repeat visit an adjacent duplicate.
    fn sorted_unique(mut seen: Vec<u64>) -> Vec<u64> {
        seen.sort_unstable();
        let duplicate = seen.windows(2).find(|pair| pair[0] == pair[1]);
        assert!(
            duplicate.is_none(),
            "pair {:?} visited more than once",
            duplicate.map(|pair| (pair[0] >> 32, pair[0] & 0xFFFF_FFFF))
        );
        seen
    }

    /// Every candidate the sweep hands the exact kernels, for one pool.
    fn every_candidate(pool: &[Segment]) -> Vec<u64> {
        let mut seen = Vec::new();
        let _ = for_each_candidate_pair(pool, single_chain, |a, b| {
            seen.push(pair_key(a, b));
            ControlFlow::Continue(())
        });
        sorted_unique(seen)
    }

    #[test]
    fn sweep_covers_every_intersecting_pair() {
        // Sizes spanning the brute branch (< SWEEP_MIN_ITEMS) and the two sweep
        // strategies, each of which is driven directly below rather than reached
        // through the run-density dispatch — so the corpus does not have to
        // contain linework that happens to select both. The oracle is quadratic
        // in n, which caps the useful size.
        for &n in &[4_usize, 20, 200, 600, 1100] {
            let pool = structured_pool(n);
            // `segments_intersect` is the exact orientation predicate, with an
            // interval filter and a stored-dyadic fallback behind it. Running
            // it on all n² pairs makes the oracle cost far more than the sweep
            // it checks, so separated envelopes — which prove non-intersection
            // outright — screen the pairs first.
            let envelopes = pool
                .iter()
                .map(|s| {
                    [
                        s.start.x.min(s.end.x),
                        s.start.y.min(s.end.y),
                        s.start.x.max(s.end.x),
                        s.start.y.max(s.end.y),
                    ]
                })
                .collect::<Vec<_>>();
            let mut truly_intersecting: Vec<u64> = Vec::new();
            for i in 0..n {
                for j in (i + 1)..n {
                    let (a, b) = (envelopes[i], envelopes[j]);
                    if a[2] < b[0] || b[2] < a[0] || a[3] < b[1] || b[3] < a[1] {
                        continue;
                    }
                    if segments_intersect(pool[i], pool[j]) {
                        truly_intersecting.push(pair_key(i, j));
                    }
                }
            }
            truly_intersecting.sort_unstable();
            {
                let visited = every_candidate(&pool);
                for &pair in &truly_intersecting {
                    assert!(
                        visited.binary_search(&pair).is_ok(),
                        "n={n}: intersecting pair {:?} dropped by the sweep",
                        (pair >> 32, pair & 0xFFFF_FFFF),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod indexed_bounds_characterization {
    use super::*;

    #[expect(clippy::unnecessary_wraps, reason = "a row is an optional bounds, and the fixtures spell present rows")]
    fn bounds(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Option<Bounds> {
        Some(Bounds::new_raw(minx, miny, maxx, maxy))
    }

    fn brute_pairs(rows: &[Option<Bounds>]) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        for left in 0..rows.len() {
            for right in (left + 1)..rows.len() {
                if rows[left]
                    .is_some_and(|left| rows[right].is_some_and(|right| left.intersects(right)))
                {
                    pairs.push((left, right));
                }
            }
        }
        pairs
    }

    fn indexed_pairs(rows: &[Option<Bounds>]) -> (Vec<(usize, usize)>, usize) {
        let mut pairs = Vec::new();
        let mut work = 0;
        let _ = for_each_indexed_overlapping_bounds_pair(rows, |left, right| {
            work += 1;
            pairs.push((left, right));
            ControlFlow::Continue(())
        });
        pairs.sort_unstable();
        (pairs, work)
    }

    #[test]
    fn indexed_bounds_match_inclusive_brute_oracle() {
        let rows = [
            bounds(0.0, 0.0, 1.0, 1.0),
            bounds(1.0, 0.0, 2.0, 1.0),
            bounds(1.0_f64.next_up(), 0.0, 2.0, 1.0),
            bounds(-0.0, -1.0, 0.0, 1.0),
            bounds(0.0, -1.0, 1.0, 1.0),
            None,
        ];
        let (mut indexed, _) = indexed_pairs(&rows);
        let mut expected = brute_pairs(&rows);
        indexed.sort_unstable();
        expected.sort_unstable();
        assert_eq!(indexed, expected);
        assert_eq!(indexed.windows(2).find(|pair| pair[0] == pair[1]), None);
    }

    #[test]
    fn indexed_bounds_short_circuit_on_first_touch() {
        let rows = std::iter::repeat_with(|| bounds(0.0, 0.0, 1.0, 1.0)).take(4096)
            .collect::<Vec<_>>();
        let mut work = 0;
        let result = for_each_indexed_overlapping_bounds_pair(&rows, |_, _| {
            work += 1;
            ControlFlow::Break(())
        });
        assert!(result.is_break());
        assert_eq!(work, 1);
    }

    #[test]
    fn indexed_bounds_work_stays_sparse_across_shapes() {
        for &count in &[64_usize, 256, 1024, 4096] {
            let separated = (0..count)
                .map(|index| {
                    let x = index as f64 * 2.0;
                    bounds(x, 0.0, x + 0.5, 1.0)
                })
                .collect::<Vec<_>>();
            let diagonal = (0..count)
                .map(|index| {
                    let position = index as f64 * 2.0;
                    bounds(position, position, position + 0.5, position + 0.5)
                })
                .collect::<Vec<_>>();
            let cross_axis = (0..count)
                .map(|index| {
                    let y = index as f64 * 2.0;
                    bounds(0.0, y, count as f64, y + 0.5)
                })
                .collect::<Vec<_>>();

            assert_eq!(indexed_pairs(&separated).1, 0, "separated n={count}");
            assert_eq!(indexed_pairs(&diagonal).1, 0, "diagonal n={count}");
            assert_eq!(indexed_pairs(&cross_axis).1, 0, "cross-axis n={count}");

            let dense = std::iter::repeat_with(|| bounds(0.0, 0.0, 1.0, 1.0)).take(count)
                .collect::<Vec<_>>();
            let mut first_hit_work = 0;
            let result = for_each_indexed_overlapping_bounds_pair(&dense, |_, _| {
                first_hit_work += 1;
                ControlFlow::Break(())
            });
            assert!(result.is_break(), "dense n={count}");
            assert_eq!(first_hit_work, 1, "dense n={count}");
        }
    }
}

#[cfg(test)]
mod run_collapse_dispatch {
    //! The pruning this sweep exists for, pinned as a candidate COUNT. Smooth
    //! linework decomposes into a couple of monotone runs, within which no pair
    //! can cross, so the run join drops essentially every candidate the flat
    //! sweep would hand the exact kernels — and linework that reverses every
    //! segment decomposes into one run each, where the two visit the same set
    //! and the flat sweep is the cheaper way to reach it. A size threshold in
    //! front of this (there were three, at 512, 512 and 1024) sent small smooth
    //! inputs down the flat path and cost them the entire reduction.
    use super::*;
    use crate::geometry::segment_index::sweep::monotone_runs;

    fn chain_of(points: &[(f64, f64)], pool: &mut Vec<Segment>) {
        for pair in points.windows(2) {
            pool.push(Segment {
                start: XY { x: pair[0].0, y: pair[0].1 },
                end: XY { x: pair[1].0, y: pair[1].1 },
            });
        }
    }

    /// A half sine wave: smooth and monotone in x, but curved, so the collapse
    /// cannot be an artifact of exactly collinear vertices.
    fn arc(x0: f64, y0: f64, n: usize) -> Vec<(f64, f64)> {
        (0..n)
            .map(|i| {
                let t = i as f64 / (n - 1) as f64;
                (x0 + 10.0 * t, y0 + 3.0 * (t * std::f64::consts::PI).sin())
            })
            .collect()
    }

    /// Alternating direction every segment: one run per segment, nothing to
    /// collapse.
    fn zigzag(x0: f64, y0: f64, n: usize) -> Vec<(f64, f64)> {
        (0..n)
            .map(|i| (x0 + i as f64 * 0.1, y0 + f64::from(u32::from(i % 2 == 0))))
            .collect()
    }

    fn pool_of(left: &[(f64, f64)], right: &[(f64, f64)]) -> Vec<Segment> {
        let mut pool = Vec::new();
        chain_of(left, &mut pool);
        chain_of(right, &mut pool);
        pool
    }

    fn visits(pool: &[Segment]) -> usize {
        let mut count = 0;
        let _ = for_each_candidate_pair(pool, single_chain, |_left, _right| {
            count += 1;
            ControlFlow::Continue(())
        });
        count
    }

    fn runs_of(pool: &[Segment]) -> usize {
        let mut runs = Vec::new();
        monotone_runs(pool, &single_chain, &mut runs);
        runs.len()
    }

    #[test]
    fn smooth_linework_collapses_at_every_size() {
        // 32 segments is far below every threshold this dispatch used to carry,
        // and is where the reduction was being thrown away.
        for n in [8_usize, 32, 128, 600] {
            let pool = pool_of(&arc(0.0, 0.0, n), &arc(0.3, -0.6, n));
            assert!(
                runs_of(&pool) <= 8,
                "n={n}: smooth linework should decompose into a handful of runs"
            );
            // The candidate count is bounded by the RUN structure, not the
            // vertex count: a per-segment sweep hands over one candidate per
            // chained adjacent pair and so grows with n, while the run join
            // stays flat as the arcs are refined. That independence is the
            // property a size threshold in front of this destroyed.
            let dispatched = visits(&pool);
            assert!(
                dispatched <= 16,
                "n={n}: {dispatched} candidates from {} segments — the run \
                 collapse is not being taken",
                pool.len()
            );
        }
    }

    #[test]
    fn linework_with_nothing_to_collapse_stays_linear() {
        // The other end: one run per segment, so the decomposition saves no
        // pair at all. The sweep still has to stay near-linear in the pool —
        // this is the case a quadratic fallback would hide in.
        for n in [8_usize, 32, 128, 600] {
            let pool = pool_of(&zigzag(0.0, 0.0, n), &zigzag(0.05, -0.4, n));
            assert_eq!(
                runs_of(&pool),
                pool.len(),
                "n={n}: alternating linework should yield one run per segment"
            );
            let dispatched = visits(&pool);
            assert!(
                dispatched <= pool.len() * 4,
                "n={n}: {dispatched} candidates from {} segments",
                pool.len()
            );
        }
    }
}
