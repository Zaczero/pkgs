use super::{WeightedCentroidMergeFallback, weighted_centroid_merge};

#[test]
fn finite_weight_nonfinite_center_requires_whole_source_replay() {
    let parts = [(1.0, f64::INFINITY, 0.0), (3.0, 2.0, 3.0)];

    let old_checked_skip = weighted_centroid_merge::<true, false>(&parts);
    assert_eq!(old_checked_skip.centroid, Some((2.0, 3.0)));

    let admitted_lineal = weighted_centroid_merge::<true, true>(&parts);
    assert_eq!(
        admitted_lineal.fallback,
        WeightedCentroidMergeFallback::ReplayWholeSource
    );
    assert_eq!(admitted_lineal.centroid, None);
}

#[test]
fn infinite_weight_precedes_nonfinite_center_admission() {
    let parts = [
        (f64::INFINITY, f64::INFINITY, f64::INFINITY),
        (3.0, 2.0, 3.0),
    ];

    let result = weighted_centroid_merge::<true, true>(&parts);

    assert_eq!(
        result.fallback,
        WeightedCentroidMergeFallback::ReplayWholeSource
    );
    assert_eq!(result.centroid, None);
}

#[test]
fn positive_normalization_underflow_remains_a_separate_outcome() {
    let parts = [(1.0e308, 0.0, 0.0), (f64::from_bits(1), 4.0, 0.0)];

    let result = weighted_centroid_merge::<true, true>(&parts);

    assert_eq!(
        result.fallback,
        WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow
    );
}
