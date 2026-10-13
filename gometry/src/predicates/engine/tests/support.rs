fn degrees() -> crate::geographic::GeographicFrame {
        crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap()
    }

fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(minx, miny),
                    Point::new_raw_xy(maxx, miny),
                    Point::new_raw_xy(maxx, maxy),
                    Point::new_raw_xy(minx, maxy),
                    Point::new_raw_xy(minx, miny),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ))
    }

fn line(start: (f64, f64), end: (f64, f64)) -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(vec![start.0, end.0], vec![start.1, end.1], None, None)
                    .unwrap(),
            )
            .expect("fixture line has two vertices"),
        )
    }

    #[derive(Clone, Copy)]
struct FarApartSupportCase {
        name: &'static str,
        left: &'static str,
        right: &'static str,
        both_supports_certified_nonempty: bool,
        matrix: Option<&'static str>,
    }

const REMOTE_RECTANGLE: &str = "POLYGON ((1000 1000,1004 1000,1004 1004,1000 1004,1000 1000))";

const FAR_APART_SUPPORT_CASES: [FarApartSupportCase; 14] = [
        FarApartSupportCase {
            name: "spike",
            left: "POLYGON ((0 0,4 0,4 4,2 4,2 6,2 4,0 4,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "collapsed polygon",
            left: "POLYGON ((0 0,2 2,4 4,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "shell equals hole",
            left: "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "zero-net bow-tie",
            left: "POLYGON ((0 0,4 4,0 4,4 0,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "twice-wound shell",
            left: "POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "external hole",
            left: "POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "empty polygon",
            left: "POLYGON EMPTY",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "collapsed-only multipolygon",
            left: "MULTIPOLYGON (((0 0,2 2,4 4,0 0)))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: false,
            matrix: None,
        },
        FarApartSupportCase {
            name: "multipolygon with collapsed member",
            left: "MULTIPOLYGON (((0 0,2 2,4 4,0 0)),((6 6,9 6,9 9,6 9,6 6)))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "collection with collapsed polygon member",
            left: "GEOMETRYCOLLECTION (POLYGON ((0 0,2 2,4 4,0 0)),LINESTRING (6 6,7 6))",
            right: REMOTE_RECTANGLE,
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "two empty supports with raw bounds",
            left: "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
            right: "POLYGON ((1000 1000,1004 1000,1004 1004,1000 1004,1000 1000),(1000 1000,1004 1000,1004 1004,1000 1004,1000 1000))",
            both_supports_certified_nonempty: false,
            matrix: Some("FFFFFFFF2"),
        },
        FarApartSupportCase {
            name: "two typed empty polygons",
            left: "POLYGON EMPTY",
            right: "POLYGON EMPTY",
            both_supports_certified_nonempty: false,
            matrix: Some("FFFFFFFF2"),
        },
        FarApartSupportCase {
            name: "disjoint points",
            left: "POINT (0 0)",
            right: "POINT (1000 1000)",
            both_supports_certified_nonempty: true,
            matrix: None,
        },
        FarApartSupportCase {
            name: "disjoint lines",
            left: "LINESTRING (0 0,4 4)",
            right: "LINESTRING (1000 1000,1004 1004)",
            both_supports_certified_nonempty: true,
            matrix: None,
        },
    ];

fn parse_wkt_shape(wkt: &str) -> Shape {
        crate::io::parse_wkt(wkt).expect("bounds-gate fixture WKT parses")
    }

fn prepared_pair(left: &Shape, right: &Shape) -> (PreparedPlan, PreparedPlan) {
        (
            PreparedPlan::new(left.clone()),
            PreparedPlan::new(right.clone()),
        )
    }

fn cartesian_predicate(predicate: Predicate, left: &Shape, right: &Shape) -> bool {
        let left = PreparedPlan::transient(left);
        let right = PreparedPlan::transient(right);
        let pair = PreparedTopologyPair::cartesian(&left, &right);
        topology_scalar_pair(&predicate.spec(), &pair).expect("Cartesian predicate succeeds")
    }

fn shared_collection(shape: Shape, depth: usize) -> Shape {
        let mut shared = shape;
        for _ in 0..depth {
            shared = Shape::geometry_collection(vec![shared.clone(), shared])
                .expect("shared collection depth is admitted");
        }
        shared
    }

fn wkb(shape: &Shape) -> Vec<u8> {
        crate::io::to_wkb(shape, None, false).expect("predicate fixture serializes")
    }
