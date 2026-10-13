#[test]
fn external_emitted_incidence_rejects_unprescribed_contacts() {
    let left = ([0, 1], seg(0.0, 0.0, 2.0, 0.0));
    for right in [
        ([2, 3], seg(1.0, -1.0, 1.0, 1.0)),
        ([2, 3], seg(1.0, 0.0, 1.0, 1.0)),
        ([2, 3], seg(1.0, 0.0, 3.0, 0.0)),
        ([1, 0], seg(2.0, 0.0, 0.0, 0.0)),
    ] {
        for items in [[left, right], [right, left]] {
            let error = certify_emitted_incidence(&items).expect_err("unprescribed contact");
            assert!(
                matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactUnexpectedContact {
                        first,
                        second,
                    })
                    if (*first, *second) == (0, 1) || (*first, *second) == (1, 0)
                ),
                "{error:?}"
            );
        }
    }
}

#[test]
fn external_emitted_incidence_accepts_disjoint_and_shared_endpoints() {
    let left = ([0, 1], seg(0.0, 0.0, 2.0, 0.0));
    for right in [
        ([2, 3], seg(0.0, 1.0, 2.0, 1.0)),
        ([1, 2], seg(2.0, 0.0, 2.0, 1.0)),
        ([1, 2], seg(2.0, 0.0, 3.0, 0.0)),
    ] {
        for items in [[left, right], [right, left]] {
            certify_emitted_incidence(&items).expect("prescribed incidence");
        }
    }
}
