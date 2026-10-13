use super::*;

#[cfg(test)]
mod mask_selection_tests {
    use super::CoordSeq;

    #[test]
    fn mask_selection_copies_disjoint_runs_and_all_ordinates() {
        let source = CoordSeq::try_from_vecs(
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            vec![11.0, 12.0, 13.0, 14.0, 15.0, 16.0],
            Some(vec![21.0, 22.0, 23.0, 24.0, 25.0, 26.0]),
            Some(vec![31.0, 32.0, 33.0, 34.0, 35.0, 36.0]),
        )
        .unwrap();
        let selected = source.select_mask(&[true, true, false, true, false, true], 4);
        assert_eq!(selected.xs(), &[1.0, 2.0, 4.0, 6.0]);
        assert_eq!(selected.ys(), &[11.0, 12.0, 14.0, 16.0]);
        assert_eq!(selected.zs().unwrap(), &[21.0, 22.0, 24.0, 26.0]);
        assert_eq!(selected.ms().unwrap(), &[31.0, 32.0, 34.0, 36.0]);
    }

    #[test]
    fn mask_selection_handles_empty_and_boundary_runs() {
        let source =
            CoordSeq::try_from_vecs(vec![1.0, 2.0, 3.0], vec![11.0, 12.0, 13.0], None, None)
                .unwrap();
        assert!(source.select_mask(&[false, false, false], 0).is_empty());
        let selected = source.select_mask(&[false, true, true], 2);
        assert_eq!(selected.xs(), &[2.0, 3.0]);
        assert_eq!(selected.ys(), &[12.0, 13.0]);
        let selected = source.select_mask(&[true, true, false], 2);
        assert_eq!(selected.xs(), &[1.0, 2.0]);
        assert_eq!(selected.ys(), &[11.0, 12.0]);
    }

    #[test]
    fn mask_selection_preserves_nan_payload_and_signed_zero_bits() {
        let nan = f64::from_bits(0x7FF8_0000_0000_0042);
        let source = CoordSeq::try_from_vecs(
            vec![-0.0, nan, 3.0],
            vec![nan, -0.0, 4.0],
            Some(vec![nan, 5.0, -0.0]),
            Some(vec![-0.0, 5.0, nan]),
        )
        .unwrap();

        let selected = source.select_mask(&[true, false, true], 2);
        let bits = |column: &[f64]| {
            column
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>()
        };
        assert_eq!(bits(selected.xs()), vec![
            (-0.0_f64).to_bits(),
            3.0_f64.to_bits()
        ]);
        assert_eq!(bits(selected.ys()), vec![nan.to_bits(), 4.0_f64.to_bits()]);
        assert_eq!(bits(selected.zs().unwrap()), vec![
            nan.to_bits(),
            (-0.0_f64).to_bits()
        ]);
        assert_eq!(bits(selected.ms().unwrap()), vec![
            (-0.0_f64).to_bits(),
            nan.to_bits()
        ]);
    }
}

#[cfg(test)]
mod select_soundness_tests {
    use super::*;

    /// Iterator that lies about its length via `size_hint` (safe trait abuse).
    struct LyingIter {
        items: Vec<usize>,
        index: usize,
        claimed: usize,
    }

    impl LyingIter {
        fn under(items: Vec<usize>, claimed: usize) -> Self {
            assert!(claimed > items.len());
            Self {
                items,
                index: 0,
                claimed,
            }
        }

        fn over(items: Vec<usize>, claimed: usize) -> Self {
            assert!(claimed < items.len());
            Self {
                items,
                index: 0,
                claimed,
            }
        }
    }

    impl Iterator for LyingIter {
        type Item = usize;

        fn next(&mut self) -> Option<usize> {
            (self.index < self.items.len()).then(|| {
                let item = self.items[self.index];
                self.index = self.index.wrapping_add(1);
                item
            })
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let remaining_claimed = self.claimed.saturating_sub(self.index);
            (remaining_claimed, Some(remaining_claimed))
        }
    }

    fn sample_seq() -> CoordSeq {
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0],
            vec![10.0, 11.0, 12.0, 13.0],
            None,
            None,
        )
        .expect("sample columns")
    }

    /// Under-yielding with an inflated `size_hint` still returns the true
    /// yield count. The hint only sizes the initial reservation.
    #[test]
    fn select_lying_under_yield_returns_true_length() {
        let seq = sample_seq();
        for claimed in [3, usize::MAX] {
            let out = seq
                .select(LyingIter::under(vec![0, 2], claimed))
                .expect("selection");
            assert_eq!(out.len(), 2);
            assert_eq!(out.xs(), &[0.0, 2.0]);
            assert_eq!(out.ys(), &[10.0, 12.0]);
        }
    }

    /// Over-yielding with a deflated `size_hint` must not write past the
    /// allocation — the result carries every yielded row.
    #[test]
    fn select_lying_over_yield_returns_all_items() {
        let seq = sample_seq();
        // Claims 2, yields [0, 1, 3].
        let out = seq
            .select(LyingIter::over(vec![0, 1, 3], 2))
            .expect("selection");
        assert_eq!(out.len(), 3);
        assert_eq!(out.xs(), &[0.0, 1.0, 3.0]);
        assert_eq!(out.ys(), &[10.0, 11.0, 13.0]);
    }

    /// Honest exact-size iterators still size the reservation from the hint.
    #[test]
    fn select_honest_exact_size_matches_source() {
        let seq = sample_seq();
        let rows = [3_usize, 1, 0];
        let out = seq.select(rows.iter().copied()).expect("selection");
        assert_eq!(out.len(), 3);
        assert_eq!(out.xs(), &[3.0, 1.0, 0.0]);
        assert_eq!(out.ys(), &[13.0, 11.0, 10.0]);
    }
}
