

    #[test]
    fn selected_edge_free_cut_avoids_a_pole_port_at_the_gap_midpoint() {
        let shapes = [
            line(&[(180.0, -80.0), (180.0, 80.0)]),
            Shape::Point(Point::new_raw_xy(0.0, 90.0)),
        ];
        let (frame, chart) = noded_chart_for_shapes(&shapes);
        let zero = ExactRational::zero();
        let half = ExactRational::from_f64(frame.longitude().half_period());
        let negative_half = ExactRational::from_f64(-frame.longitude().half_period());
        let intervals = chart_edge_longitude_intervals(frame, &chart).unwrap();
        assert!(!intervals.is_empty());
        assert!(intervals.iter().all(|(low, high)| {
            low.try_cmp(high).unwrap().is_eq()
                && (low.try_cmp(&half).unwrap().is_eq()
                    || low.try_cmp(&negative_half).unwrap().is_eq())
        }));
        assert!(chart_has_cut_port(frame, &chart, &zero).unwrap());

        assert!(
            select_edge_free_cut(frame, &chart).unwrap().is_some(),
            "the seam-only edge support leaves an open longitude gap"
        );
        let cut = select_cut(frame, &chart).unwrap();
        assert!(!chart_has_cut_port(frame, &chart, &cut).unwrap());
        assert!(
            !cut.try_cmp(&zero).unwrap().is_eq(),
            "the midpoint collides with the north-pole port"
        );
    }

    #[test]
    fn no_edge_free_cut_fallback_avoids_a_pole_port() {
        let shapes = [
            line(&[(-180.0, 0.0), (-60.0, 0.0), (60.0, 0.0), (180.0, 0.0)]),
            Shape::Point(Point::new_raw_xy(-120.0, 90.0)),
        ];
        let (frame, chart) = noded_chart_for_shapes(&shapes);
        let pole_port_longitude = ExactRational::from_f64(-120.0);
        let north_pole = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North));
        assert!(
            chart.ports.iter().any(|port| {
                port.point.x.try_cmp(&pole_port_longitude).unwrap().is_eq()
                    && port.point.y.try_cmp(&north_pole).unwrap().is_eq()
            }),
            "the fixture includes a north-pole chart port at the port-gap midpoint"
        );
        assert!(select_edge_free_cut(frame, &chart).unwrap().is_none());

        let cut = select_cut(frame, &chart).unwrap();
        assert!(!chart_has_cut_port(frame, &chart, &cut).unwrap());
        assert!(
            !cut.try_cmp(&pole_port_longitude).unwrap().is_eq(),
            "the port-gap fallback must include pole-port longitudes"
        );
    }