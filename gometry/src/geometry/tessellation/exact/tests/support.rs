fn check_cocircular_underflow(exponent: i32) {
    let scale = 2.0_f64.powi(exponent);
    let points = [
        XY::new(5.0 * scale, 0.0),
        XY::new(3.0 * scale, 4.0 * scale),
        XY::new(0.0, 5.0 * scale),
        XY::new(-3.0 * scale, 4.0 * scale),
    ];
    let [a, b, c, d] = points;
    assert_eq!(super::incircle_dyadic(a, b, c, d).unwrap(), ExactSign::Zero);
    if let Some(sign) = super::incircle_float(a, b, c, d) {
        assert_eq!(sign, ExactSign::Zero, "scale exponent {exponent}");
    }
    assert_eq!(super::incircle(a, b, c, d).unwrap(), ExactSign::Zero);
}
