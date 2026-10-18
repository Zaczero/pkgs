use std::ops::ControlFlow;

use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::geometry::segment_index::sweep::for_each_indexed_overlapping_bounds_pair_with_work;
use crate::geometry::segment_index::{
    for_each_candidate_pair, for_each_indexed_overlapping_bounds_pair,
    for_each_overlapping_bounds_cross_pair, for_each_overlapping_bounds_pair, single_chain,
};
use crate::geometry::{Bounds, Segment, XY, segments_intersect};
use crate::work::Unmetered;

#[test]
fn cross_bounds_sweep_preserves_each_side_ordinal() {
    for count in [2, 32, 2_048] {
        let left = (0..count)
            .map(|index| {
                let x = index as f64 * 10.0;
                Bounds::new_raw(x, x, x + 2.0, x + 2.0)
            })
            .collect::<Vec<_>>();
        let right = (0..count)
            .map(|index| {
                let x = index as f64 * 10.0 - 1.0;
                Bounds::new_raw(x, x + 2.0, x + 2.0, x + 3.0)
            })
            .collect::<Vec<_>>();
        let expected = (0..count).map(|index| (index, index)).collect::<Vec<_>>();
        let mut actual = Vec::new();
        let flow = for_each_overlapping_bounds_cross_pair(&left, &right, |left, right| {
            actual.push((left, right));
            ControlFlow::<()>::Continue(())
        })
        .unwrap();
        assert!(matches!(flow, ControlFlow::Continue(())));
        actual.sort_unstable();
        assert_eq!(actual, expected);
    }
}

#[cfg(test)]
mod sweep_characterization {
    //! Golden contract for the candidate-pair sweep: it visits a SUPERSET of
    //! every truly-intersecting pair (no real interaction is ever dropped) and
    //! never visits an unordered pair twice. The exact kernels are the final filter, so the sweep's only
    //! correctness duty is completeness; this test locks it so any later change
    //! to the sweep's input representation that loses a candidate fails loudly.
    use super::*;

    fn structured_pool(n: usize) -> Vec<Segment> {
        let groups = n.saturating_sub(4).div_ceil(8);
        let xmax = (groups.wrapping_mul(16).wrapping_add(12)) as f64;
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
            let x = group.wrapping_mul(16) as f64;
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
        let duplicate = seen.array_windows::<2>().find(|[a, b]| a == b);
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
        let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
            for_each_candidate_pair(pool, single_chain, |a, b| {
                seen.push(pair_key(a, b));
                ControlFlow::Continue(())
            })
            .expect("sweep indexes fit their structural widths");
        sorted_unique(seen)
    }

    #[test]
    fn sweep_covers_every_intersecting_pair() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        // Cross the six-item brute/sweep boundary and retain four complete
        // groups of the eight structured contact families.
        for &n in &[4_usize, 6, 20, 36] {
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

    #[test]
    fn oblique_projection_keeps_contacts_between_parallel_runs() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        let mut pool = (0..64)
            .map(|index| {
                let y = f64::from(index) * 2.0;
                Segment {
                    start: XY::new(0.0, y),
                    end: XY::new(64.0, y + 8.0),
                }
            })
            .collect::<Vec<_>>();
        pool.push(Segment {
            start: XY::new(32.0, -1.0),
            end: XY::new(32.0, 130.0),
        });

        let mut runs = Vec::new();
        super::super::sweep::monotone_runs_with_work(
            &pool,
            &single_chain,
            &mut runs,
            &mut Unmetered,
        )
        .expect("the test runs fit their structural width");
        let mut candidates = Vec::new();
        let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
            super::super::sweep::candidate_pairs_over_staged_runs::<true, _>(
                &pool,
                &runs,
                &mut |left, right| {
                    candidates.push(pair_key(left, right));
                    ControlFlow::Continue(())
                },
            )
            .expect("the projection sweep fits its structural widths");
        let visited = sorted_unique(candidates);
        for left in 0..pool.len() {
            for right in (left + 1)..pool.len() {
                if segments_intersect(pool[left], pool[right]) {
                    let pair = pair_key(left, right);
                    assert!(
                        visited.binary_search(&pair).is_ok(),
                        "crossing pair {:?} dropped by the oblique envelope filter",
                        (left, right),
                    );
                }
            }
        }
    }

    #[test]
    fn for_each_candidate_pair_stops_on_break() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        for count in [4, 20, 4096] {
            let pool = structured_pool(count);
            let mut visited = 0;
            let flow = for_each_candidate_pair(&pool, single_chain, |_, _| {
                visited += 1;
                ControlFlow::Break(String::from("contact"))
            })
            .expect("sweep indexes fit their structural widths");
            assert_eq!(flow, ControlFlow::Break(String::from("contact")));
            assert_eq!(visited, 1);
        }
    }
}

#[cfg(test)]
mod indexed_bounds_characterization {
    use super::*;

    #[expect(
        clippy::unnecessary_wraps,
        reason = "a row is an optional bounds, and the fixtures spell present rows"
    )]
    fn bounds(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Option<Bounds> {
        Some(Bounds::new_raw(minx, miny, maxx, maxy))
    }

    fn brute_pairs(rows: &[Option<Bounds>]) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        let mut remaining = rows;
        let mut left_index = 0_usize;
        while let Some((left, right_rows)) = remaining.split_first() {
            let mut right_index = left_index.wrapping_add(1);
            for right in right_rows {
                if left.is_some_and(|left| right.is_some_and(|right| left.intersects(right))) {
                    pairs.push((left_index, right_index));
                }
                right_index = right_index.wrapping_add(1);
            }
            left_index = left_index.wrapping_add(1);
            remaining = right_rows;
        }
        pairs
    }

    fn indexed_pairs(rows: &[Option<Bounds>]) -> (Vec<(usize, usize)>, usize) {
        let mut pairs = Vec::new();
        let mut work = 0_usize;
        let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
            for_each_indexed_overlapping_bounds_pair(rows, |left, right| {
                work = work.wrapping_add(1);
                pairs.push((left, right));
                ControlFlow::Continue(())
            })
            .unwrap();
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
        assert_eq!(indexed.array_windows::<2>().find(|[a, b]| a == b), None);
    }

    #[test]
    fn banded_bounds_visit_each_multiband_pair_once_in_input_order() {
        let mut rows = Vec::with_capacity(4096);
        rows.extend([
            bounds(0.0, -1.0, 2.0, 1.0),
            bounds(1.0, -1.0, 3.0, 1.0),
            bounds(2.5, -1.0, 3.5, 1.0),
        ]);
        rows.extend([bounds(4.5, -3.0, 5.0, -2.0), bounds(6.5, 2.0, 7.0, 3.0)]);
        rows.extend((0..4091).map(|index| {
            let min_x = 8.5 + f64::from(index) * 2.0;
            bounds(min_x, -1.0, min_x + 0.5, 1.0)
        }));

        let mut pairs = Vec::new();
        let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
            for_each_indexed_overlapping_bounds_pair(&rows, |left, right| {
                pairs.push((left, right));
                ControlFlow::Continue(())
            })
            .unwrap();
        assert_eq!(pairs, [(0, 1), (1, 2)]);

        let mut funded_pairs = Vec::new();
        let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        let flow = for_each_indexed_overlapping_bounds_pair_with_work(
            &rows,
            &mut budget,
            |left, right, _work| {
                funded_pairs.push((left, right));
                Ok(ControlFlow::<std::convert::Infallible>::Continue(()))
            },
        )
        .unwrap();
        assert_eq!(flow, ControlFlow::Continue(()));
        assert_eq!(funded_pairs, [(0, 1), (1, 2)]);
    }

    #[test]
    fn indexed_bounds_short_circuit_on_first_touch() {
        let rows = std::iter::repeat_with(|| bounds(0.0, 0.0, 1.0, 1.0))
            .take(4096)
            .collect::<Vec<_>>();
        let mut work = 0;
        let result = for_each_indexed_overlapping_bounds_pair(&rows, |_, _| {
            work += 1;
            ControlFlow::Break(String::from("bounds contact"))
        })
        .unwrap();
        assert_eq!(result, ControlFlow::Break(String::from("bounds contact")));
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

            let dense = std::iter::repeat_with(|| bounds(0.0, 0.0, 1.0, 1.0))
                .take(count)
                .collect::<Vec<_>>();
            let mut first_hit_work = 0;
            let result = for_each_indexed_overlapping_bounds_pair(&dense, |_, _| {
                first_hit_work += 1;
                ControlFlow::Break(())
            })
            .unwrap();
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
    use crate::geometry::segment_index::sweep::monotone_runs_with_work;

    fn chain_of(points: &[(f64, f64)], pool: &mut Vec<Segment>) {
        for &[from, to] in points.array_windows::<2>() {
            pool.push(Segment {
                start: XY {
                    x: from.0,
                    y: from.1,
                },
                end: XY { x: to.0, y: to.1 },
            });
        }
    }

    /// A half sine wave: smooth and monotone in x, but curved, so the collapse
    /// cannot be an artifact of exactly collinear vertices.
    fn arc(x0: f64, y0: f64, n: usize) -> Vec<(f64, f64)> {
        (0..n)
            .map(|i| {
                let t = i as f64 / n.wrapping_sub(1) as f64;
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
        let mut count = 0_usize;
        let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
            for_each_candidate_pair(pool, single_chain, |_left, _right| {
                count = count.wrapping_add(1);
                ControlFlow::Continue(())
            })
            .expect("sweep indexes fit their structural widths");
        count
    }

    fn runs_of(pool: &[Segment]) -> usize {
        let mut runs = Vec::new();
        monotone_runs_with_work(pool, &single_chain, &mut runs, &mut crate::work::Unmetered)
            .expect("run indexes fit u32");
        runs.len()
    }

    fn ordered_candidates(pool: &[Segment]) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
            for_each_candidate_pair(pool, single_chain, |left, right| {
                pairs.push((left, right));
                ControlFlow::Continue(())
            })
            .expect("sweep indexes fit their structural widths");
        pairs
    }

    #[test]
    fn empty_two_edge_chain_envelope_visits_no_pairs() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        let left = [(0.0, 0.0), (2.0, 0.0), (2.0, 2.0)];
        let right = [
            (0.25, 0.25),
            (0.5, 0.5),
            (0.75, 0.75),
            (1.0, 1.0),
            (1.25, 1.25),
        ];
        for (scale_x, scale_y) in [(1.0, 2.0), (2.0, 1.0), (-1.0, 2.0), (-2.0, 1.0)] {
            let transform = |points: &[(f64, f64)]| {
                points
                    .iter()
                    .map(|&(x, y)| (x * scale_x, y * scale_y))
                    .collect::<Vec<_>>()
            };
            let (left, right) = (transform(&left), transform(&right));
            for pool in [pool_of(&left, &right), pool_of(&right, &left)] {
                assert!(ordered_candidates(&pool).is_empty());
            }
        }
    }

    #[test]
    fn two_edge_chain_keeps_closed_boundary_contacts() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        let left = [(0.0, 0.0), (2.0, 0.0), (2.0, 2.0)];
        for (right, expected) in [
            (
                [
                    (0.25, -0.75),
                    (0.75, -0.25),
                    (1.25, 0.25),
                    (1.75, 0.75),
                    (2.25, 1.25),
                ],
                [(0, 3), (1, 5)],
            ),
            (
                [
                    (2.0, -2.0),
                    (2.0, -1.5),
                    (2.0, -1.0),
                    (2.0, -0.5),
                    (2.0, 0.0),
                ],
                [(0, 5), (1, 5)],
            ),
        ] {
            for (scale_x, scale_y) in [(1.0, 2.0), (2.0, 1.0), (-1.0, 2.0), (-2.0, 1.0)] {
                let transform = |points: &[(f64, f64)]| {
                    points
                        .iter()
                        .map(|&(x, y)| (x * scale_x, y * scale_y))
                        .collect::<Vec<_>>()
                };
                let (left, right) = (transform(&left), transform(&right));
                let mut pairs = ordered_candidates(&pool_of(&left, &right));
                pairs.sort_unstable();
                assert_eq!(pairs, expected);
                let mut pairs = ordered_candidates(&pool_of(&right, &left));
                pairs.sort_unstable();
                let mut reversed = expected.map(|(short, long)| (long - 2, short + 4));
                reversed.sort_unstable();
                assert_eq!(pairs, reversed);
            }
        }
    }

    #[test]
    fn collinear_chains_keep_ordered_callbacks_and_early_break() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        let left = [(0.0, 0.0), (4.0, 0.0), (8.0, 0.0)];
        let right = [(0.0, 0.0), (2.0, 0.0), (4.0, 0.0), (6.0, 0.0), (8.0, 0.0)];
        for (pool, expected) in [
            (pool_of(&left, &right), [
                (0, 2),
                (0, 3),
                (1, 3),
                (0, 4),
                (1, 4),
                (1, 5),
            ]),
            (pool_of(&right, &left), [
                (0, 4),
                (1, 4),
                (1, 5),
                (2, 4),
                (2, 5),
                (3, 5),
            ]),
        ] {
            assert_eq!(ordered_candidates(&pool), expected);
            let mut prefix = Vec::new();
            let flow = for_each_candidate_pair(&pool, single_chain, |left, right| {
                prefix.push((left, right));
                if prefix.len() == 3 {
                    ControlFlow::Break("third contact")
                } else {
                    ControlFlow::Continue(())
                }
            })
            .expect("sweep indexes fit their structural widths");
            assert_eq!(flow, ControlFlow::Break("third contact"));
            assert_eq!(prefix, expected[..3]);
        }
    }

    #[test]
    fn smooth_linework_collapses_at_every_size() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
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
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
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

#[cfg(test)]
mod sweep_entry_scratch {
    use super::*;

    fn overlapping_boxes(count: usize) -> Vec<Bounds> {
        (0..count)
            .map(|index| {
                let x = index as f64 * 0.5;
                Bounds::new_raw(x, 0.0, x + 1.0, 1.0)
            })
            .collect()
    }

    fn crossing_segments(count: usize) -> Vec<Segment> {
        (0..count)
            .map(|index| {
                let x = index as f64;
                Segment {
                    start: XY::new(x, 0.0),
                    end: XY::new(x + 1.0, 1.0),
                }
            })
            .collect()
    }

    fn bounds_pairs(bounds: &[Bounds]) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
            for_each_overlapping_bounds_pair(bounds, |left, right| {
                pairs.push((left, right));
                ControlFlow::Continue(())
            })
            .expect("bounds sweep indexes fit their structural widths");
        pairs
    }

    #[test]
    fn nested_candidate_sweep_does_not_drop_outer_bounds_pairs() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        let bounds = overlapping_boxes(8);
        let segments = crossing_segments(8);
        let expected = bounds_pairs(&bounds);
        assert!(expected.len() > 1);

        let mut nested = Vec::new();
        let mut inner_visits = 0_usize;
        let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
            for_each_overlapping_bounds_pair(&bounds, |left, right| {
                let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
                    for_each_candidate_pair(&segments, single_chain, |_, _| {
                        inner_visits = inner_visits.wrapping_add(1);
                        ControlFlow::Continue(())
                    })
                    .expect("inner candidate sweep");
                nested.push((left, right));
                ControlFlow::Continue(())
            })
            .expect("outer bounds sweep");
        assert_eq!(nested, expected);
        assert!(inner_visits > 0);
    }

    #[test]
    fn nested_early_break_releases_the_shared_lease() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        let bounds = overlapping_boxes(8);
        let segments = crossing_segments(8);
        let expected = bounds_pairs(&bounds);

        let mut first = None;
        let flow = for_each_overlapping_bounds_pair(&bounds, |left, right| {
            let inner =
                for_each_candidate_pair(&segments, single_chain, |_, _| ControlFlow::Break(()))
                    .expect("inner candidate sweep");
            assert_eq!(inner, ControlFlow::Break(()));
            first = Some((left, right));
            ControlFlow::Break(())
        })
        .expect("outer bounds sweep");
        assert!(matches!(flow, ControlFlow::Break(())));
        assert_eq!(first, expected.first().copied());
        assert_eq!(bounds_pairs(&bounds), expected);
    }
}
