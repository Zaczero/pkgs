    #[test]
fn stored_query_axis_order_preserves_topological_identity() {
        let first_nan = f64::from_bits(0x7FF8_0000_0000_0001);
        let second_nan = f64::from_bits(0x7FF8_0000_0000_0002);
        let query = StoredQuery(XY::new(first_nan, 0.0));
        assert_eq!(
            query.compare_axis(QueryAxis::X, first_nan).unwrap(),
            Ordering::Equal
        );
        assert_ne!(
            query.compare_axis(QueryAxis::X, second_nan).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            query.compare_axis(QueryAxis::Y, -0.0).unwrap(),
            Ordering::Equal
        );

        let finite = StoredQuery(XY::new(1.0, -0.0));
        assert!(finite.axis_relation(QueryAxis::X, 2.0).unwrap().query_below);
        assert!(!finite.axis_relation(QueryAxis::X, 0.0).unwrap().query_below);
        assert!(finite.axis_relation(QueryAxis::Y, 0.0).unwrap().equal);
    }
