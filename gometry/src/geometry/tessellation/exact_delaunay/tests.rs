use super::mesh::PERIMETER_STEPS;
use super::ordering::{INSERTION_ORDER, InsertionOrder};
use super::{
    EMPTY, ExactMesh, FaceId, FaceSide, HalfedgeId, Located, Mesh, MeshCoordinates,
    canonical_incircle, compare_f64, compare_xy, exact_delaunay, next_halfedge, site_xy,
};
use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact;
use crate::geometry::{Point, XY};

fn points(xy: &[(f64, f64)]) -> Vec<Point> {
    xy.iter().map(|&(x, y)| Point::new_raw_xy(x, y)).collect()
}

fn random(n: usize, seed: u64) -> Vec<Point> {
    let mut s = seed;
    let mut next = move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        (s >> 11) as f64 / (1_u64 << 53) as f64
    };
    std::iter::repeat_with(|| Point::new_raw_xy(next() * 1000.0, next() * 1000.0))
        .take(n)
        .collect()
}

#[test]
fn symbolic_lift_ties_are_alternating_and_geometric_zeros_remain_zeros() {
    let [a, b, c, d] = [
        XY::new(0.0, 0.0),
        XY::new(1.0, 0.0),
        XY::new(1.0, 1.0),
        XY::new(0.0, 1.0),
    ];
    assert_eq!(exact::incircle(a, b, c, d).unwrap(), ExactSign::Zero);
    assert_eq!(canonical_incircle(a, b, c, d).unwrap(), ExactSign::Positive);
    assert_eq!(canonical_incircle(b, a, c, d).unwrap(), ExactSign::Negative);
    assert_eq!(canonical_incircle(a, c, b, d).unwrap(), ExactSign::Negative);
    assert_eq!(canonical_incircle(a, b, d, c).unwrap(), ExactSign::Negative);
}

struct FallbackScanCoordinates {
    points: [XY; 4],
    orientation_calls: std::cell::Cell<usize>,
}

impl MeshCoordinates for FallbackScanCoordinates {
    type Query = XY;
    type Key = [u64; 2];

    fn query(&self, site: usize) -> Self::Query {
        *self
            .points
            .get(site)
            .expect("fallback test queries an admitted square site")
    }

    fn key(&self, query: Self::Query) -> Self::Key {
        let xy = |site| self.query(site);
        xy.key(query)
    }

    fn compare(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        compare_xy(left, right)
    }

    fn compare_x(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        compare_f64(left.x, right.x)
    }

    fn compare_y(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        compare_f64(left.y, right.y)
    }

    fn orient(&self, _points: [Self::Query; 3]) -> Result<ExactSign> {
        let call = self.orientation_calls.get().wrapping_add(1);
        self.orientation_calls.set(call);
        match call {
            3 | 4 | 7 => Ok(ExactSign::Negative),
            8 => Err(crate::error::ResourceError::SizeOverflow.into()),
            // If fallback scanning wrongly swallows call 8's error, the
            // next face receives an inside classification.
            _ => Ok(ExactSign::Positive),
        }
    }

    fn incircle(&self, [a, b, c, d]: [Self::Query; 4]) -> Result<ExactSign> {
        exact::incircle(a, b, c, d)
    }

    fn compare_axis_spans(
        &self,
        [min_x, max_x]: [Self::Query; 2],
        [min_y, max_y]: [Self::Query; 2],
    ) -> Result<std::cmp::Ordering> {
        let xy = |site| self.query(site);
        xy.compare_axis_spans([min_x, max_x], [min_y, max_y])
    }

    fn locality_xy(&self, site: usize) -> XY {
        self.query(site)
    }
}

#[test]
fn location_propagates_arithmetic_error_from_the_bounded_fallback_scan() {
    // The seeded square has adjacent faces 0 and 1 across a shared edge.
    // Inject signs that walk 0 -> 1 -> 0 until the step limit selects the
    // fallback scan, then fail on that scan's first face. This is a
    // control-flow fault injection, not a geometric input fixture.
    let coordinates = FallbackScanCoordinates {
        points: [
            XY::new(0.0, 0.0),
            XY::new(2.0, 0.0),
            XY::new(2.0, 2.0),
            XY::new(0.0, 2.0),
        ],
        orientation_calls: std::cell::Cell::new(0),
    };
    let mesh = Mesh::seed(&[0, 1, 2, 3], 4);
    let error = mesh
        .locate_bounded(&coordinates, FaceId::from_index(0), XY::new(10.0, 10.0), &[
        ])
        .unwrap_err();
    assert_eq!(coordinates.orientation_calls.get(), 8);
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
    ));
}

#[test]
fn public_delaunay_triangles_are_canonical_across_insertion_orders() {
    use std::fmt::Write as _;

    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyModule};

    struct RestoreOrder(InsertionOrder);
    impl Drop for RestoreOrder {
        fn drop(&mut self) {
            INSERTION_ORDER.with(|order| order.set(self.0));
        }
    }

    crate::test_support::initialize_python();
    let _restore = RestoreOrder(INSERTION_ORDER.with(std::cell::Cell::get));
    Python::attach(|py| {
        let module = PyModule::new(py, "gometry._lib").unwrap();
        crate::lib(&module).unwrap();
        let grid = (0..8)
            .flat_map(|x| (0..8).map(move |y| format!("({x} {y})")))
            .collect::<Vec<_>>()
            .join(",");

        let mut holes = String::new();
        for index in 0..8 {
            let x = f64::from(index);
            write!(
                &mut holes,
                ",({} 1,{} 1,{} 2,{} 2,{} 1)",
                x + 0.2,
                x + 0.8,
                x + 0.8,
                x + 0.2,
                x + 0.2
            )
            .unwrap();
        }
        let fixtures = [
            (format!("MULTIPOINT({grid})"), "delaunay"),
            (
                format!("POLYGON((0 0,9 0,9 3,0 3,0 0){holes})"),
                "constrained",
            ),
            (
                "POLYGON((0 0,8 0,8 8,4 3,0 8,0 0))".to_owned(),
                "constrained",
            ),
        ];
        for (wkt, method) in fixtures {
            let polygon = module.getattr("from_wkt").unwrap().call1((wkt,)).unwrap();
            let options = PyDict::new(py);
            options.set_item("method", method).unwrap();
            let mut expected = None;
            for order in [
                InsertionOrder::Original,
                InsertionOrder::Brio,
                InsertionOrder::Reversed,
            ] {
                INSERTION_ORDER.with(|cell| cell.set(order));
                let output = polygon
                    .call_method("triangulate", (), Some(&options))
                    .unwrap()
                    .call_method0("to_wkt")
                    .unwrap()
                    .extract::<Vec<String>>()
                    .unwrap();
                if let Some(expected) = &expected {
                    assert_eq!(&output, expected);
                } else {
                    expected = Some(output);
                }
            }
        }

        let globals = PyDict::new(py);
        globals.set_item("gm", &module).unwrap();
        py.run(
                c"import math, random
rng = random.Random(0xCD7)
corpus = []
for case in range(200):
    n = rng.randint(4, 8)
    step = math.tau / n
    offset = rng.random() * math.tau
    ring = []
    for index in range(n):
        angle = offset + index * step + rng.uniform(-0.15, 0.15) * step
        radius = rng.randint(20, 80)
        ring.append((float(round(radius * math.cos(angle))), float(round(radius * math.sin(angle)))))
    polygon = gm.Polygon(ring)
    corpus.append((polygon, {'method': 'constrained'}))
",
                Some(&globals),
                None,
            )
            .unwrap();
        let corpus = globals
            .get_item("corpus")
            .unwrap()
            .unwrap()
            .extract::<Vec<(Bound<'_, PyAny>, Bound<'_, PyDict>)>>()
            .unwrap();
        for (case, (polygon, options)) in corpus.into_iter().enumerate() {
            let mut expected = None;
            for order in [
                InsertionOrder::Original,
                InsertionOrder::Brio,
                InsertionOrder::Reversed,
            ] {
                INSERTION_ORDER.with(|cell| cell.set(order));
                let output = polygon
                    .call_method("triangulate", (), Some(&options))
                    .unwrap()
                    .call_method0("to_wkt")
                    .unwrap()
                    .extract::<Vec<String>>()
                    .unwrap();
                if let Some(expected) = &expected {
                    assert_eq!(&output, expected, "corpus observation {case}");
                } else {
                    expected = Some(output);
                }
            }
        }
    });
}

#[test]
fn public_refinement_is_canonical_across_insertion_and_ring_orders() {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyModule};

    struct RestoreOrder(InsertionOrder);
    impl Drop for RestoreOrder {
        fn drop(&mut self) {
            INSERTION_ORDER.with(|order| order.set(self.0));
        }
    }
    crate::test_support::initialize_python();
    let _restore = RestoreOrder(INSERTION_ORDER.with(std::cell::Cell::get));
    Python::attach(|py| {
        let module = PyModule::new(py, "gometry._lib").unwrap();
        crate::lib(&module).unwrap();
        let globals = PyDict::new(py);
        globals.set_item("gm", &module).unwrap();
        py.run(
            c"corpus = []
rings = [
    [(-75,16),(-63,-42),(-11,-75),(31,-39),(39,-9),(25,20),(4,70),(-18,23)],
    [(0,0),(20,0),(20,20),(0,20)],
    [(0,0),(8,0),(8,8),(4,3),(0,8)],
    [(0,0),(20,0),(20,2),(0,1)],
    [(33,-5),(43,32),(5,67),(-58,36),(-22,-7),(-15,-47),(26,-60)],
    [(-19,34),(-38,5),(-8,-45),(57,-55),(66,34)],
    [(54,59),(-1,46),(-64,46),(-74,-28),(-22,-71),(12,-21),(36,-5)],
    [(-50,-54),(4,-46),(46,-44),(22,2),(42,49),(-10,61),(-17,15),(-22,-3)],
    [(-37,6),(-36,-17),(-13,-42),(18,-25),(55,-20),(35,16),(5,40),(-38,63)],
]
for ring in rings:
    area = gm.Polygon(ring).area / 8
    for quality in [{'max_area': area}, {'min_angle': 20}, {'min_angle': 20, 'max_area': area}]:
        variants = []
        for reverse in (False, True):
            vertices = list(reversed(ring)) if reverse else ring
            for offset in range(len(vertices)):
                variants.append(gm.Polygon(vertices[offset:] + vertices[:offset]))
        corpus.append((variants, {'method': 'constrained', **quality}))
corpus.append(([gm.Polygon(rings[0])], {'method': 'constrained', 'max_area': 1117.5625}))
",
            Some(&globals),
            None,
        )
        .unwrap();
        let corpus = globals
            .get_item("corpus")
            .unwrap()
            .unwrap()
            .extract::<Vec<(Vec<Bound<'_, PyAny>>, Bound<'_, PyDict>)>>()
            .unwrap();
        for (case, (variants, options)) in corpus.into_iter().enumerate() {
            let mut expected = None;
            for polygon in variants {
                for order in [
                    InsertionOrder::Original,
                    InsertionOrder::Brio,
                    InsertionOrder::Reversed,
                ] {
                    INSERTION_ORDER.with(|cell| cell.set(order));
                    let output = polygon
                        .call_method("triangulate", (), Some(&options))
                        .unwrap()
                        .call_method0("to_wkt")
                        .unwrap()
                        .extract::<Vec<String>>()
                        .unwrap();
                    assert!(!output.is_empty(), "refinement case {case}");
                    if let Some(expected) = &expected {
                        assert_eq!(&output, expected, "refinement case {case}, {order:?}");
                    } else {
                        expected = Some(output);
                    }
                }
            }
        }
    });
}

fn assert_refinement_order_identity(wkt: &str, option: &str, value: f64) {
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    struct RestoreOrder(InsertionOrder);
    impl Drop for RestoreOrder {
        fn drop(&mut self) {
            INSERTION_ORDER.with(|order| order.set(self.0));
        }
    }
    crate::test_support::initialize_python();
    let _restore = RestoreOrder(INSERTION_ORDER.with(std::cell::Cell::get));
    Python::attach(|py| {
        let module = PyModule::new(py, "gometry._lib").unwrap();
        crate::lib(&module).unwrap();
        let polygon = module.getattr("from_wkt").unwrap().call1((wkt,)).unwrap();
        let options = PyDict::new(py);
        options.set_item("method", "constrained").unwrap();
        options.set_item(option, value).unwrap();
        let mut expected = None;
        for order in [
            InsertionOrder::Original,
            InsertionOrder::Brio,
            InsertionOrder::Reversed,
        ] {
            INSERTION_ORDER.with(|cell| cell.set(order));
            let output = polygon
                .call_method("triangulate", (), Some(&options))
                .unwrap()
                .call_method0("to_wkt")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap();
            assert!(!output.is_empty(), "{wkt}, {option}={value}");
            if let Some(expected) = &expected {
                assert_eq!(&output, expected, "{wkt}, {option}={value}, {order:?}");
            } else {
                expected = Some(output);
            }
        }
    });
}

macro_rules! refinement_order_case {
    ($name:ident, $wkt:literal, $option:literal, $value:expr) => {
        #[test]
        fn $name() {
            assert_refinement_order_identity($wkt, $option, $value);
        }
    };
}

refinement_order_case!(
    area_1024_order_identity,
    "POLYGON((0 0,2 0,2 2,0 2,0 0))",
    "max_area",
    4.0 / 1024.0
);
refinement_order_case!(
    area_2048_order_identity,
    "POLYGON((0 0,2 0,2 2,0 2,0 0))",
    "max_area",
    4.0 / 2048.0
);
refinement_order_case!(
    area_4096_order_identity,
    "POLYGON((0 0,2 0,2 2,0 2,0 0))",
    "max_area",
    4.0 / 4096.0
);
refinement_order_case!(
    area_8192_order_identity,
    "POLYGON((0 0,2 0,2 2,0 2,0 0))",
    "max_area",
    4.0 / 8192.0
);
refinement_order_case!(
    angle_1024_order_identity,
    "POLYGON((0 0,256 0,256 1,0 1,0 0))",
    "min_angle",
    20.0
);
refinement_order_case!(
    angle_2048_order_identity,
    "POLYGON((0 0,512 0,512 1,0 1,0 0))",
    "min_angle",
    20.0
);
refinement_order_case!(
    angle_4096_order_identity,
    "POLYGON((0 0,1024 0,1024 1,0 1,0 0))",
    "min_angle",
    20.0
);
refinement_order_case!(
    angle_8192_order_identity,
    "POLYGON((0 0,2048 0,2048 1,0 1,0 0))",
    "min_angle",
    20.0
);
refinement_order_case!(
    angle_16384_order_identity,
    "POLYGON((0 0,4096 0,4096 1,0 1,0 0))",
    "min_angle",
    20.0
);

#[test]
fn outside_insertions_replace_only_the_visible_hull_chain() {
    let points: Vec<_> = [
        (0.0, 0.0),
        (2.0, 0.0),
        (2.0, 2.0),
        (0.0, 2.0),
        (-2.0, 1.0),
        (1.0, -3.0),
        (5.0, 2.0),
        (2.0, 6.0),
    ]
    .map(|(x, y)| Point::new_raw_xy(x, y))
    .into();
    let xy = |site| site_xy(&points, site);
    let mut mesh = Mesh::try_delaunay(&xy, 4).unwrap().unwrap();
    let mut constrained = vec![false; mesh.edge_count()];
    for site in 4..points.len() {
        let visible = mesh.outside_edge(&xy, xy(site)).unwrap().unwrap();
        let insertion = mesh
            .insert_outside(&xy, site, visible, &mut constrained)
            .unwrap();
        mesh.try_legalize_constrained_from(&xy, &mut constrained, insertion.legalization)
            .unwrap();
        let expected = Mesh::try_delaunay(&xy, site.wrapping_add(1))
            .unwrap()
            .unwrap();
        let face_set = |mesh: &Mesh| {
            let mut triangles: Vec<_> = mesh
                .face_ids()
                .filter(|&face| mesh.alive(face))
                .map(|face| {
                    let mut corners = mesh.triangle(face);
                    corners.sort_unstable();
                    corners
                })
                .collect();
            triangles.sort_unstable();
            triangles
        };
        assert_eq!(face_set(&mesh), face_set(&expected));
        for (&start, &end) in mesh.hull.iter().zip(mesh.hull.iter().cycle().skip(1)) {
            assert_eq!(mesh.hull.previous(end), start);
            let edge = mesh.directed_slot(start, end).unwrap();
            assert_eq!(mesh.twin(edge), EMPTY);
        }
    }
}

fn grid(side: usize) -> Vec<Point> {
    (0..side)
        .flat_map(|y| (0..side).map(move |x| Point::new_raw_xy(x as f64, y as f64)))
        .collect()
}

fn assert_halfedges_consistent(mesh: &ExactMesh) {
    for e in 0..mesh.halfedges.len() {
        let twin = mesh.halfedges[e];
        if twin == EMPTY {
            continue;
        }
        assert_eq!(mesh.halfedges[twin], e, "twin of twin is self at {e}");
        assert_eq!(
            mesh.triangles[e],
            mesh.triangles[next_halfedge(twin)],
            "twins run opposite at {e}"
        );
        assert_eq!(mesh.triangles[next_halfedge(e)], mesh.triangles[twin]);
    }
}

fn assert_delaunay(mesh: &ExactMesh, points: &[Point]) {
    let xy = |s: usize| points[s].xy();
    for (face, (vertices, twins)) in mesh
        .triangles
        .as_chunks::<3>()
        .0
        .iter()
        .zip(mesh.halfedges.as_chunks::<3>().0)
        .enumerate()
    {
        let [a, b, c] = *vertices;
        assert_eq!(
            exact::orient2d(xy(a), xy(b), xy(c)),
            ExactSign::Positive,
            "triangle {face} is counter-clockwise"
        );
        for (side, &twin) in FaceSide::ALL.into_iter().zip(twins) {
            if twin == EMPTY {
                continue;
            }
            let edge = HalfedgeId::in_face(FaceId::from_index(face), side);
            let opposite = mesh.triangles[HalfedgeId::from_index(twin).prev().index()];
            let sign = exact::incircle(xy(a), xy(b), xy(c), xy(opposite)).unwrap();
            assert_ne!(
                sign,
                ExactSign::Positive,
                "opposite vertex {opposite} inside circumcircle of {face}"
            );
            assert_ne!(
                canonical_incircle(xy(a), xy(b), xy(c), xy(opposite)).unwrap(),
                ExactSign::Positive,
                "opposite vertex {opposite} violates the canonical lift of {face}"
            );
            assert_eq!(
                sign == ExactSign::Zero,
                mesh.cocircular[edge.index()],
                "cocircular flag at {}",
                edge.index()
            );
        }
    }
}

fn triangle_set(triangles: &[usize]) -> Vec<[usize; 3]> {
    let mut set: Vec<[usize; 3]> = triangles
        .as_chunks::<3>()
        .0
        .iter()
        .map(|&t| {
            let mut t = t;
            t.sort_unstable();
            t
        })
        .collect();
    set.sort_unstable();
    set
}

#[test]
fn random_sites_are_delaunay_consistent_and_match_the_current_engine() {
    for seed in [1_u64, 7, 42, 1234] {
        let pts = random(400, seed);
        let mesh = exact_delaunay(&pts).unwrap();
        assert_halfedges_consistent(&mesh);
        assert_delaunay(&mesh, &pts);
        // Random sites have no cocircular quads, so the Delaunay
        // triangulation is unique and must equal the current engine's.
        assert!(
            !mesh.cocircular.iter().any(|&f| f),
            "random sites should not be cocircular"
        );
        let current = super::super::delaunay_triangulation(&pts).unwrap();
        assert_eq!(
            triangle_set(&mesh.triangles),
            triangle_set(&current.triangles),
            "seed {seed}"
        );
        assert_eq!(mesh.hull, current.hull);
        // Euler for a triangulated convex polygon: T = 2n - h - 2.
        assert_eq!(
            mesh.triangles.len() / 3,
            2 * pts.len() - mesh.hull.len() - 2
        );
    }
}

#[test]
fn hull_only_regular_polygons_are_delaunay() {
    // Every site is a hull corner, so nothing is ever inserted and the
    // seed fan has to be legalized on its own: a regular n-gon's fan from
    // one corner is far from Delaunay.
    for n in 5..=40_usize {
        let points: Vec<Point> = (0..n)
            .map(|i| {
                let t = std::f64::consts::TAU * i as f64 / n as f64;
                Point::new_raw_xy(t.cos(), t.sin())
            })
            .collect();
        let mesh = exact_delaunay(&points).unwrap();
        assert_eq!(mesh.triangles.len() / 3, n - 2, "n={n} triangles");
        assert_eq!(mesh.hull.len(), n, "n={n} hull");
        assert_halfedges_consistent(&mesh);
        assert_delaunay(&mesh, &points);
    }
}

#[test]
fn a_grid_is_delaunay_with_every_diagonal_flagged_cocircular() {
    let pts = grid(20);
    let mesh = exact_delaunay(&pts).unwrap();
    assert_halfedges_consistent(&mesh);
    assert_delaunay(&mesh, &pts);
    assert_eq!(
        mesh.hull.len(),
        76,
        "all grid boundary sites are on the hull"
    );
    assert_eq!(
        mesh.triangles.as_chunks::<3>().0.len(),
        722,
        "the 400-site grid with a 76-site hull has 722 faces"
    );
    let boundary_axis = [0.0_f64.to_bits(), 19.0_f64.to_bits()];
    let expected_hull: std::collections::HashSet<[u64; 2]> = pts
        .iter()
        .filter(|point| {
            boundary_axis.contains(&point.x.to_bits()) || boundary_axis.contains(&point.y.to_bits())
        })
        .map(|point| [point.x.to_bits(), point.y.to_bits()])
        .collect();
    let actual_hull: std::collections::HashSet<[u64; 2]> = mesh
        .hull
        .iter()
        .map(|&site| {
            let point = pts.get(site).expect("hull site is an input site");
            [point.x.to_bits(), point.y.to_bits()]
        })
        .collect();
    assert_eq!(
        actual_hull, expected_hull,
        "the hull retains every boundary site"
    );
    let center = Point::new_raw_xy(10.0, 10.0);
    for (&start, &end) in mesh.hull.iter().zip(mesh.hull.iter().cycle().skip(1)) {
        let start = pts.get(start).expect("hull site is an input site").xy();
        let end = pts.get(end).expect("hull site is an input site").xy();
        let dx = (start.x - end.x).abs().to_bits();
        let dy = (start.y - end.y).abs().to_bits();
        assert!(
            (dx == 1.0_f64.to_bits() && dy == 0.0_f64.to_bits())
                || (dx == 0.0_f64.to_bits() && dy == 1.0_f64.to_bits()),
            "adjacent hull sites must be consecutive grid points"
        );
        assert_eq!(
            exact::orient2d(start, end, center.xy()),
            ExactSign::Positive,
            "the hull cycle is counter-clockwise"
        );
    }
    // Every interior diagonal of a unit square is cocircular.
    let flagged = mesh.cocircular.iter().filter(|&&f| f).count() / 2;
    assert_eq!(flagged, 19 * 19, "one cocircular diagonal per grid cell");
}

/// Duplicate sites are deduplicated by identity and the distinct sites
/// are triangulated, hull corners and all; the previous engine refused
/// them with an empty triangulation.
#[test]
fn duplicate_sites_triangulate_the_distinct_points() {
    let dup = points(&[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 0.0),
        (0.0, 1.0),
        (1.0, 1.0),
        (0.5, 0.5),
        (0.5, 0.5),
    ]);
    let mesh = exact_delaunay(&dup).unwrap();
    assert_halfedges_consistent(&mesh);
    assert_delaunay(&mesh, &dup);
    assert_eq!(mesh.hull, super::super::exact_corner_hull_cycle(&dup));
    // Five distinct sites, four on the hull: 2*5 - 4 - 2 = 4 triangles.
    assert_eq!(mesh.triangles.len() / 3, 4);
    let public = super::super::delaunay_triangulation(&dup).unwrap();
    assert_eq!(
        public.triangles.len() / 3,
        4,
        "the public entry point triangulates the distinct sites"
    );
}

#[test]
fn collinear_input_matches_the_current_hull() {
    let line = points(&[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]);
    let mesh = exact_delaunay(&line).unwrap();
    assert!(mesh.triangles.is_empty());
    assert_eq!(
        mesh.hull,
        super::super::delaunay_triangulation(&line).unwrap().hull
    );
}

#[test]
fn sites_on_existing_edges_and_vertices_insert_exactly() {
    // A square, then its centre, then midpoints of the two diagonals'
    // halves (on edges), then a repeat of the centre (on a vertex).
    let pts = points(&[
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (0.0, 4.0),
        (2.0, 2.0),
        (1.0, 1.0),
        (3.0, 3.0),
        (2.0, 2.0),
    ]);
    let mesh = exact_delaunay(&pts).unwrap();
    assert_halfedges_consistent(&mesh);
    assert_delaunay(&mesh, &pts);
    assert_eq!(mesh.triangles.len() / 3, 2 * 7 - 4 - 2);
}

#[test]
fn late_center_insertion_walks_large_cavities_in_boundary_order() {
    for n in [32, 128, 512] {
        let mut pts: Vec<_> = (0..n)
            .map(|i| {
                let angle = std::f64::consts::TAU * i as f64 / n as f64;
                Point::new_raw_xy(angle.cos(), angle.sin())
            })
            .collect();
        pts.push(Point::new_raw_xy(0.0, 0.0));
        let xy = |site: usize| pts[site].xy();
        let hull: Vec<_> = (0..n).collect();
        let mut mesh = Mesh::seed(&hull, pts.len());
        mesh.try_legalize(&xy).unwrap();
        let located = mesh.locate(&xy, FaceId::from_index(0), xy(n)).unwrap();
        let (_, changed) = mesh
            .try_insert_with(&xy, n, located, None, |mut cavity, slots| {
                assert_eq!(cavity.len(), n - 2);
                assert_eq!(slots.len(), n);
                cavity.extend_from_slice(slots);
                cavity
            })
            .unwrap();
        assert_eq!(changed.len(), 2 * n - 2);
        let steps = PERIMETER_STEPS.with(std::cell::Cell::get);
        assert_eq!(steps, 3 * (n - 2));
        eprintln!("late center: boundary={n}, cavity={}, steps={steps}", n - 2);
        let result = mesh.finish(&xy).unwrap();
        assert_halfedges_consistent(&result);
        assert_delaunay(&result, &pts);
    }
}

#[test]
fn insertion_reports_cavity_faces_before_replacement_slots() {
    let pts = points(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (1.0, 0.5)]);
    let xy = |site: usize| pts[site].xy();
    let mut mesh = Mesh::seed(&[0, 1, 2, 3], pts.len());
    mesh.try_legalize(&xy).unwrap();
    let located = mesh.locate(&xy, FaceId::from_index(0), xy(4)).unwrap();
    let (last, changed) = mesh
        .try_insert_with(&xy, 4, located, None, |mut cavity, slots| {
            cavity.extend_from_slice(slots);
            cavity
        })
        .unwrap();
    assert_eq!(
        changed.iter().map(|face| face.index()).collect::<Vec<_>>(),
        [1, 0, 0, 1, 2, 3]
    );
    assert_eq!(last.index(), 3);
    let repeated = mesh.locate(&xy, last, xy(4)).unwrap();
    let (_, changed) = mesh
        .try_insert_with(&xy, 4, repeated, None, |mut cavity, slots| {
            cavity.extend_from_slice(slots);
            cavity
        })
        .unwrap();
    assert!(changed.is_empty());
}

#[test]
fn cavity_stamp_rollover_keeps_unvisited_neighbor_reachable() {
    let pts = points(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (1.0, 0.5)]);
    let xy = |site: usize| pts[site].xy();
    let seed = || {
        let mut mesh = Mesh::seed(&[0, 1, 2, 3], pts.len());
        mesh.try_legalize(&xy).unwrap();
        mesh
    };
    let mut ordinary = seed();
    let mut wrapped = seed();
    wrapped.epoch = u32::MAX;
    wrapped.stamp.fill(1);
    let located = ordinary.locate(&xy, FaceId::from_index(0), xy(4)).unwrap();
    assert!(matches!(located, Located::Inside(_)));
    ordinary
        .try_insert_with(&xy, 4, located, None, |_, _| ())
        .unwrap();
    wrapped
        .try_insert_with(&xy, 4, located, None, |_, _| ())
        .unwrap();
    let ordinary = ordinary.finish(&xy).unwrap();
    let wrapped = wrapped.finish(&xy).unwrap();
    assert_delaunay(&ordinary, &pts);
    assert_delaunay(&wrapped, &pts);
    assert_eq!(
        triangle_set(&ordinary.triangles),
        triangle_set(&wrapped.triangles)
    );
    assert_eq!(ordinary.cocircular, wrapped.cocircular);
}
