    #[test]
    fn fixed_size_nested_vertex_input_does_not_repeat_member_scans() {
        let depth = 128;
        let prefix = "GEOMETRYCOLLECTION (".repeat(depth);
        let suffix = ")".repeat(depth);
        let mut coordinates = "0.1 0.2, ".repeat(8191);
        coordinates.push_str("0.1 0.2");
        let body_length = 0x0002_0000_usize;
        let fixed = prefix.len() + "LINESTRING (".len() + coordinates.len() + suffix.len() + 1;
        let input = format!(
            "{prefix}LINESTRING ({}{padding}){suffix}",
            coordinates,
            padding = " ".repeat(body_length.saturating_sub(fixed)),
        );

        let (_, work) = parse_wkt_counted(&input);
        assert!(
            work <= input.len().saturating_mul(2),
            "cursor advanced {} times for {} UTF-8 bytes",
            work,
            input.len()
        );
    }

    #[test]
    fn nested_collections_advance_a_linear_number_of_cursor_bytes() {
        let mut input = "GEOMETRYCOLLECTION (".repeat(128);
        input.push_str("POINT (0.1 0.2)");
        input.push_str(&")".repeat(128));

        let (_, work) = parse_wkt_counted(&input);
        assert!(
            work <= input.len().saturating_mul(2),
            "cursor advanced {} times for {} UTF-8 bytes",
            work,
            input.len()
        );
    }

    fn parse_wkt_counted(value: &str) -> (Shape, usize) {
        WKT_CURSOR_WORK.with(|work| work.set(0));
        let shape = parse_wkt(value).expect("counted WKT parses");
        let work = WKT_CURSOR_WORK.with(Cell::get);
        (shape, work)
    }
