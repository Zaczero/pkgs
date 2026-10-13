    #[test]
    fn coordinate_cursor_preserves_float_bits_and_ordinates() {
        let line = parse_wkt("LINESTRING (-0.0 1.2345678901234567, 1e308 -1e-308)")
            .expect("ASCII coordinates parse");
        let Shape::LineString(line) = line else {
            panic!("expected line string");
        };
        assert_eq!(line.as_coords().xs()[0].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(
            line.as_coords().ys()[0].to_bits(),
            1.234_567_890_123_456_7_f64.to_bits()
        );
        assert_eq!(line.as_coords().xs()[1].to_bits(), 1e308_f64.to_bits());
        assert_eq!(line.as_coords().ys()[1].to_bits(), (-1e-308_f64).to_bits());

        let point = parse_wkt("POINT ZM (1 2 3 4)").expect("tagged point parses");
        let Shape::Point(point) = point else {
            panic!("expected point");
        };
        assert_eq!(
            (point.x, point.y, point.z(), point.m()),
            (1.0, 2.0, Some(3.0), Some(4.0))
        );
    }

    #[test]
    fn coordinate_dispatch_preserves_unicode_whitespace_compatibility() {
        let point = parse_wkt("POINT ZM (1\u{00a0}2\u{2003}3\u{2028}4)")
            .expect("Unicode whitespace remains accepted");
        let Shape::Point(point) = point else {
            panic!("expected point");
        };
        assert_eq!(
            (point.x, point.y, point.z(), point.m()),
            (1.0, 2.0, Some(3.0), Some(4.0))
        );

        let line = parse_wkt("LINESTRING (1\u{2003}2, 3\u{2028}4)")
            .expect("Unicode whitespace remains accepted in lists");
        let Shape::LineString(line) = line else {
            panic!("expected line string");
        };
        assert_eq!(line.as_coords().xs(), &[1.0, 3.0]);
        assert_eq!(line.as_coords().ys(), &[2.0, 4.0]);
    }

    #[test]
    fn coordinate_dispatch_rejects_non_whitespace_unicode_in_numbers() {
        let error = parse_wkt("POINT (1\u{00e9} 2)").expect_err("Unicode character is invalid");
        assert!(error.to_string().contains("x coordinate must be numeric"));

        let error = parse_wkt("POINT (1\u{00a0}not-a-number)")
            .expect_err("Unicode whitespace cannot make a bad number valid");
        assert!(error.to_string().contains("y coordinate must be numeric"));
    }
