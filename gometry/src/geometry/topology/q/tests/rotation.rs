

    #[test]
    fn exact_q_orders_adjacent_source_rays_by_their_cross_product() {
        let high = f64::from_bits(1.0_f64.to_bits() + 1);
        let source = Shape::geometry_collection(vec![
            line(&[(0.0, 0.0), (1.0, high)]),
            line(&[(0.0, 0.0), (1.0, 1.0)]),
        ])
        .unwrap();
        assert_eq!(
            compare_source_ray_order(degrees(), &source, (0.0, 0.0), (1.0, high), (1.0, 1.0)),
            Ordering::Greater,
        );
        assert_eq!(
            compare_source_ray_order(degrees(), &source, (0.0, 0.0), (1.0, 1.0), (1.0, high)),
            Ordering::Less,
        );
    }

    #[test]
    fn exact_q_orders_equal_longitude_pole_tangents_and_reverses_south() {
        let high = f64::from_bits(1.0_f64.to_bits() + 1);
        let north = Shape::geometry_collection(vec![
            line(&[(0.0, 90.0), (high, 89.0)]),
            line(&[(0.0, 90.0), (1.0, 89.0)]),
        ])
        .unwrap();
        assert_eq!(
            compare_source_ray_order(degrees(), &north, (0.0, 90.0), (high, 89.0), (1.0, 89.0)),
            Ordering::Greater,
        );
        assert_eq!(
            compare_source_ray_order(degrees(), &north, (0.0, 90.0), (1.0, 89.0), (high, 89.0)),
            Ordering::Less,
        );

        let south = Shape::geometry_collection(vec![
            line(&[(0.0, -90.0), (high, -89.0)]),
            line(&[(0.0, -90.0), (1.0, -89.0)]),
        ])
        .unwrap();
        assert_eq!(
            compare_source_ray_order(degrees(), &south, (0.0, -90.0), (high, -89.0), (1.0, -89.0)),
            Ordering::Less,
        );
        assert_eq!(
            compare_source_ray_order(degrees(), &south, (0.0, -90.0), (1.0, -89.0), (high, -89.0)),
            Ordering::Greater,
        );
    }