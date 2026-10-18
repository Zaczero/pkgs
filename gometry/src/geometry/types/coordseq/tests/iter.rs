use super::*;

#[cfg(test)]
mod coord_iter_tests {
    use super::*;

    fn sequence(zs: Option<Vec<f64>>, ms: Option<Vec<f64>>) -> CoordSeq {
        CoordSeq::try_from_vecs(vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], zs, ms)
            .expect("test coordinate sequence")
    }

    #[rstest::rstest]
    #[case::sequence_none_none(sequence(None, None))]
    #[case::sequence_some_vec_7_0_8_0_9_0_none(sequence(Some(vec![7.0, 8.0, 9.0]), None))]
    #[case::sequence_none_some_vec_10_0_11_0_12_0(sequence(None, Some(vec![10.0, 11.0, 12.0])))]
    #[case::sequence_some_vec_7_0_8_0_9_0_some_vec_10_0_11_0_12_0(sequence(Some(vec![7.0, 8.0, 9.0]), Some(vec![10.0, 11.0, 12.0])))]
    fn specialized_iterators_exhaust_exactly_for_every_axis_variant(#[case] seq: CoordSeq) {
        let expected = (0..seq.len())
            .map(|index| seq.point_at(index))
            .collect::<Vec<_>>();
        let mut iter = seq.iter();
        assert_eq!(iter.len(), 3);
        assert_eq!(iter.next(), Some(expected[0]));
        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next_back(), Some(expected[2]));
        assert_eq!(iter.len(), 1);
        assert_eq!(iter.next(), Some(expected[1]));
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
        assert_eq!(seq.iter().collect::<Vec<_>>(), expected);
    }
}
