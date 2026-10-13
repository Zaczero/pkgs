use super::exact_lineal_centroid_runs;

#[test]
fn exact_line_sum_recovers_subnormal_moment_products() {
    let delta = 2.0_f64.powi(-540);
    let first_x = [-0.5 * delta, 0.5 * delta];
    let first_y = [0.0, 0.0];
    let second_x = [3.5 * delta, 4.5 * delta];
    let second_y = [0.0, 0.0];

    assert_eq!((first_x[1] - first_x[0]).to_bits(), delta.to_bits());
    assert_eq!((second_x[1] - second_x[0]).to_bits(), delta.to_bits());
    assert_eq!((delta * (4.0 * delta)).to_bits(), 0.0_f64.to_bits());

    let centroid = exact_lineal_centroid_runs(|visit| {
        visit(&first_x, &first_y);
        visit(&second_x, &second_y);
    })
    .expect("positive lineal measure");
    assert_eq!(centroid.0.to_bits(), (2.0 * delta).to_bits());
    assert_eq!(centroid.1.to_bits(), 0.0_f64.to_bits());
}
