#[derive(Clone, Copy)]
struct CountedStoredQuery<'a> {
    query: StoredQuery,
    axis_probes: &'a Cell<usize>,
}

impl PointQuery for CountedStoredQuery<'_> {
    fn compare_axis(self, axis: QueryAxis, value: f64) -> Result<Ordering> {
        self.query.compare_axis(axis, value)
    }

    fn axis_relation(self, axis: QueryAxis, value: f64) -> Result<AxisRelation> {
        self.axis_probes.set(self.axis_probes.get().wrapping_add(1));
        self.query.axis_relation(axis, value)
    }

    fn orientation(self, start: XY, end: XY) -> Result<ExactSign> {
        self.query.orientation(start, end)
    }

    fn index_coordinate(self, axis: QueryAxis) -> f64 {
        self.query.index_coordinate(axis)
    }

    fn misses_height(self, start: f64, end: f64) -> bool {
        self.query.misses_height(start, end)
    }
}

fn shape(wkt: &str) -> Shape {
    crate::io::parse_wkt(wkt).expect("test WKT admits")
}

fn location(wkt: &str, x: f64, y: f64) -> Loc {
    cartesian_point_location(&shape(wkt), Point::new_raw_xy(x, y))
}

fn assert_streaming_matches_reference(shape: &Shape, point: Point) {
    let expected = ReferenceSourceSupport::from_shape(shape).location(point);
    let actual = cartesian_point_location(shape, point);
    assert_eq!(actual, expected, "{shape:?} at ({}, {})", point.x, point.y);
    assert_eq!(
        cartesian_point_covers(shape, point),
        expected != Loc::Exterior,
        "coverage for {shape:?} at ({}, {})",
        point.x,
        point.y
    );
}
