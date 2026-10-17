use std::cmp::Ordering;

use super::super::exact_delaunay::MeshCoordinates as _;
use super::*;
use crate::error::{ErrorKind, Result};
use crate::geographic::GeographicFrame;
use crate::geometry::tessellation::test_support::points;
use crate::geometry::{GeometryErrorKind, open_xy_cycle_magnitude};
include!("tests/support.rs");

#[test]
fn retained_cdt_refinement_uses_exact_sites_after_chart_rounding() {
    let frame = GeographicFrame::crs84();
    let next = 0.2_f64.next_up();
    let source_points = [
        Point::new_raw_xy(0.2, 0.0),
        Point::new_raw_xy(next, 0.0),
        Point::new_raw_xy(0.2, 1.0),
    ];
    let sites = source_points
        .iter()
        .enumerate()
        .map(|(ordinal, &point)| {
            ChartPointSite::from_lifted_source(frame, point, ordinal, 1).unwrap()
        })
        .collect::<Vec<_>>();
    let coordinates = CdtCoordinates::retained(frame, &sites, &sites).unwrap();

    assert_eq!(
        coordinates.site_xy(0).x,
        coordinates.site_xy(1).x,
        "adjacent retained chart X values have one rounded proposal"
    );
    let triangle = [0, 1, 2];
    let queries = triangle.map(|site| coordinates.site_query(site));
    assert_eq!(coordinates.orient(queries).unwrap(), ExactSign::Positive);
    assert!(
        coordinates
            .triangle_exceeds_area(triangle, 1.0e-18)
            .unwrap()
    );
    assert!(coordinates.circumcenter(triangle).unwrap().is_some());
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn public_constrained_source_chain_work_is_linear() {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyModule};

    crate::test_support::initialize_python();
    Python::attach(|py| {
        let module = PyModule::new(py, "gometry._lib").unwrap();
        crate::lib(&module).unwrap();
        let options = PyDict::new(py);
        options.set_item("method", "constrained").unwrap();
        for count in [128_u32, 512] {
            let points = (0..count)
                .map(|index| {
                    let angle = f64::from(index) * std::f64::consts::TAU / f64::from(count);
                    format!("{} {}", angle.cos(), angle.sin())
                })
                .collect::<Vec<_>>();
            let ring = points
                .iter()
                .chain(points.first())
                .cloned()
                .collect::<Vec<_>>()
                .join(",");
            let polygon = module
                .getattr("from_wkt")
                .unwrap()
                .call1((format!("POLYGON(({ring}))"),))
                .unwrap();
            let _ = take_refinement_work_counters();
            let triangles = polygon
                .call_method("triangulate", (), Some(&options))
                .unwrap();
            assert_eq!(triangles.len().unwrap(), count as usize - 2);
            let work = take_refinement_work_counters();
            assert!(
                work.source_piece_visits <= count as usize * 2,
                "{count} source edges inspected {} pieces",
                work.source_piece_visits
            );
        }
    });
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn public_refinement_quality_work_scales_with_output() {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyModule};

    crate::test_support::initialize_python();
    Python::attach(|py| {
        let module = PyModule::new(py, "gometry._lib").unwrap();
        crate::lib(&module).unwrap();
        let polygon = module
            .getattr("from_wkt")
            .unwrap()
            .call1(("POLYGON((0 0,2 0,2 2,0 2,0 0))",))
            .unwrap();
        let mut observations = Vec::new();
        for count in [128_u32, 512] {
            let options = PyDict::new(py);
            options.set_item("method", "constrained").unwrap();
            options
                .set_item("max_area", 4.0 / f64::from(count))
                .unwrap();
            let _ = take_refinement_work_counters();
            let triangles = polygon
                .call_method("triangulate", (), Some(&options))
                .unwrap();
            let output = triangles.len().unwrap();
            assert!(output >= count as usize);
            let work = take_refinement_work_counters();
            assert_eq!(work.bad_face_scans, 1);
            assert_eq!(work.constraint_reconstruction_passes, 1);
            assert!(work.quality_checks <= output * 16, "{work:?}");
            observations.push(work.quality_checks);
        }
        assert!(observations[1] <= observations[0] * 5, "{observations:?}");
    });
}

#[test]
fn a_constraint_becomes_an_edge_and_survives() {
    // Four corners of a square plus a point off the diagonal that makes
    // the Delaunay diagonal the "wrong" one for the constraint.
    let pts = points(&[
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (2.0, 8.0),
    ]);
    let mesh = ConstrainedMesh::build(&pts, &[[0, 2]]).unwrap();
    assert!(
        mesh.mesh.edge_slot(0, 2).is_some(),
        "the constraint 0-2 is an edge"
    );
    let slot = mesh.mesh.edge_slot(0, 2).unwrap();
    assert!(mesh.constrained[slot.index()]);
}

#[test]
fn a_constraint_crossing_many_edges_becomes_an_edge() {
    // Points alternate above and below the axis, so the Delaunay edges
    // between them cross the constraint 0-1 many times.
    let mut xy = vec![(0.0, 0.0), (12.0, 0.0)];
    for step in 1..12 {
        let side = if step % 2 == 0 { 1.0 } else { -1.0 };
        xy.push((f64::from(step), side * (0.5 + 0.1 * f64::from(step % 3))));
    }
    let pts = points(&xy);
    let mesh = ConstrainedMesh::build(&pts, &[[0, 1]]).unwrap();
    assert_flags_match(&mesh, &[[0, 1]]);
}

#[test]
fn flips_carry_earlier_constraint_flags() {
    let pts = points(&[
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (2.0, 8.0),
    ]);
    let edges = [[0, 1], [1, 2], [2, 3], [3, 0], [0, 2]];
    let mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
    assert_flags_match(&mesh, &edges);
}

#[test]
fn crossing_constraints_are_an_error() {
    let pts = points(&[
        (0.0, 0.0),
        (10.0, 10.0),
        (10.0, 0.0),
        (0.0, 10.0),
        (5.0, 1.0),
    ]);
    let result = ConstrainedMesh::build_with_sources(
        &pts,
        &[[0, 1], [2, 3]]
            .iter()
            .enumerate()
            .map(|(id, &[start_site, end_site])| SourceConstraint {
                id,
                source_root: id,
                start: pts[start_site],
                end: pts[end_site],
                start_site,
                end_site,
                start_occurrence: start_site,
                end_occurrence: end_site,
            })
            .collect::<Vec<_>>(),
        vec![SourceRingStart::new(0, RingOwner::new(0, 0))],
        CdtCoordinates::raw(&pts, &pts),
    );
    assert!(matches!(
        result.as_ref(),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::Triangulation(message))
                    if message.as_ref() == "constrained triangulation boundaries cross"
            )
    ));
}

#[test]
fn parity_flood_finds_the_inside_of_a_square_with_a_hole() {
    let outer = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)];
    let hole = [(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)];
    let pts = points(&outer.iter().chain(hole.iter()).copied().collect::<Vec<_>>());
    let ring = |base: usize| {
        (0..4)
            .map(|i| [base + i, base + (i + 1) % 4])
            .collect::<Vec<_>>()
    };
    let edges: Vec<[usize; 2]> = ring(0).into_iter().chain(ring(4)).collect();
    let mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
    let inside = mesh.inside_faces();
    let xy = |s: usize| pts[s].xy();
    for (face, &kept) in mesh.mesh.face_ids().zip(&inside) {
        if !mesh.mesh.alive(face) {
            continue;
        }
        let [a, b, c] = mesh.mesh.triangle(face);
        let cx = (xy(a).x + xy(b).x + xy(c).x) / 3.0;
        let cy = (xy(a).y + xy(b).y + xy(c).y) / 3.0;
        let in_hole = (4.0..6.0).contains(&cx) && (4.0..6.0).contains(&cy);
        assert_eq!(
            kept,
            !in_hole,
            "face {} centroid ({cx}, {cy})",
            face.index()
        );
    }
}

#[test]
fn refinement_meets_an_area_target_within_budget_and_reports_when_it_cannot() {
    let mut pts = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
    let edges: Vec<[usize; 2]> = (0..4).map(|i| [i, (i + 1) % 4]).collect();
    let mut mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
    assert!(
        mesh.refine(&mut pts, None, Some(25.0), 10_000).unwrap(),
        "a generous budget completes"
    );
    let inside = mesh.inside_faces();
    for (face, &kept) in mesh.mesh.face_ids().zip(&inside) {
        if kept && mesh.mesh.alive(face) {
            let [a, b, c] = mesh.mesh.triangle(face);
            assert!(
                open_xy_cycle_magnitude(&[pts[a].xy(), pts[b].xy(), pts[c].xy()]).get() <= 25.0
            );
        }
    }
    let mut pts2 = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
    let mut mesh2 = ConstrainedMesh::build(&pts2, &edges).unwrap();
    assert!(
        !mesh2.refine(&mut pts2, None, Some(0.5), 3).unwrap(),
        "three vertices cannot reach 0.5 area"
    );
}

#[test]
fn refinement_keeps_construction_recipes_and_exact_boundary_roots() {
    let mut points = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
    let edges: Vec<[usize; 2]> = (0..4).map(|index| [index, (index + 1) % 4]).collect();
    let mut mesh = ConstrainedMesh::build(&points, &edges).unwrap();
    let refinement = super::super::CdtRefinement::On {
        min_angle: None,
        max_area: Some(crate::Positive::try_new("max_area", 25.0).unwrap()),
    };
    assert!(mesh.refine(&mut points, None, Some(25.0), 10_000).unwrap());

    assert!(mesh.provenance.iter().any(|origin| {
        matches!(origin, NodeProvenance::InteriorRefinement {
            construction: RefinementConstruction::Circumcenter,
            ..
        })
    }));
    let half = exact::ExactRational::zero()
        .try_midpoint(&exact::ExactRational::one())
        .unwrap();
    assert!(mesh.split_certificates.iter().any(|certificate| {
        certificate
            .parameter
            .try_cmp(&half)
            .is_ok_and(std::cmp::Ordering::is_eq)
    }));

    let plan = mesh.finish_plan(points, refinement, true).unwrap();
    assert!(
        plan.provenance
            .iter()
            .any(|origin| { matches!(origin, NodeProvenance::ConstraintSplit { .. }) })
    );
    assert!(
        plan.constraint_pieces
            .iter()
            .any(|piece| { piece.lo.try_cmp(&half).is_ok_and(std::cmp::Ordering::is_eq) })
    );
    assert!(
        plan.constraint_pieces
            .iter()
            .any(|piece| { piece.hi.try_cmp(&half).is_ok_and(std::cmp::Ordering::is_eq) })
    );
    plan.validate_certificate().unwrap();
}

#[test]
fn constraint_recovery_composes_exact_root_parameters_through_a_site() {
    let frame = GeographicFrame::crs84();
    let points = points(&[
        (170.0, 0.0),
        (-170.0, 0.0),
        (177.0, 0.0),
        (190.0, 4.0),
        (170.0, 4.0),
    ]);
    let chart_sites = points
        .iter()
        .enumerate()
        .map(|(ordinal, &point)| {
            ChartPointSite::from_lifted_source(
                frame,
                point,
                ordinal,
                i64::from(matches!(ordinal, 1 | 3)),
            )
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let start = *points.first().expect("fixture has its first source site");
    let end = *points.get(1).expect("fixture has its second source site");
    let source = SourceConstraint {
        id: 0,
        source_root: 0,
        start,
        end,
        start_site: 0,
        end_site: 1,
        start_occurrence: 0,
        end_occurrence: 1,
    };
    let sources = [source];
    let source_rings = [SourceRingStart::new(0, RingOwner::new(0, 0))];
    let source_locations = chart_sites.iter().copied().take(2).collect::<Vec<_>>();
    let coordinates = CdtCoordinates::retained(frame, &chart_sites, &source_locations).unwrap();
    let mut mesh =
        ConstrainedMesh::build_with_sources(&points, &sources, source_rings.to_vec(), coordinates)
            .unwrap()
            .expect("the five source sites have a non-collinear mesh");
    let pieces = mesh
        .constraint_pieces
        .take_ordered(&sources, &[], &source_rings, &mesh.coordinates)
        .unwrap();
    let third = exact::ExactRational::new(
        exact::ExactDyadic::from_f64(7.0),
        exact::ExactDyadic::from_f64(20.0),
    )
    .unwrap();
    assert_eq!(pieces.len(), 2);
    let first_piece = pieces
        .first()
        .expect("source recovery emits the first interval");
    let second_piece = pieces
        .get(1)
        .expect("source recovery emits the second interval");
    assert_eq!(
        first_piece
            .lo
            .try_cmp(&exact::ExactRational::zero())
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(first_piece.hi.try_cmp(&third).unwrap(), Ordering::Equal);
    assert_eq!(second_piece.lo.try_cmp(&third).unwrap(), Ordering::Equal);
    assert_eq!(
        second_piece
            .hi
            .try_cmp(&exact::ExactRational::one())
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        (
            first_piece.start_site,
            first_piece.end_site,
            second_piece.end_site,
        ),
        (0, 2, 1)
    );
}

#[test]
fn refinement_initializes_global_state_once() {
    let _ = take_refinement_work_counters();
    let mut pts = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
    let edges: Vec<[usize; 2]> = (0..4).map(|i| [i, (i + 1) % 4]).collect();
    let mut mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
    assert!(mesh.refine(&mut pts, None, Some(100.0), 100).unwrap());
    let counters = take_refinement_work_counters();
    assert_eq!(counters.inside_label_passes, 1);
    assert_eq!(counters.bad_face_scans, 1);
    assert_eq!(counters.constraint_reconstruction_passes, 0);
    assert!(counters.quality_checks <= mesh.mesh.triangle_count() * 8);
}

#[test]
fn rounded_source_midpoint_is_rejected_before_mesh_mutation() {
    let next = 1.0_f64.next_up();
    let mut pts = points(&[(1.0, 0.0), (next, 1.0), (1.0, 0.5)]);
    let before = pts.clone();
    let edges = vec![[0, 1], [1, 2], [2, 0]];
    let mut mesh = ConstrainedMesh::build(&pts, &edges).expect("non-collinear source");
    let error = mesh
        .refine(&mut pts, None, Some(1.0e-30), 32)
        .expect_err("the rounded midpoint collides with an existing source vertex");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
            operation: "triangulate",
            ..
        })
    ));
    assert_eq!(pts, before, "rejected source split does not append a site");
}

#[test]
fn rounded_point_inside_the_adjacent_face_is_a_boundary_split_location() {
    let next = 1.0_f64.next_up();
    let pts = points(&[(1.0, 0.0), (next, 1.0), (0.0, 0.5)]);
    let mesh = ConstrainedMesh::build(&pts, &[[0, 1], [1, 2], [2, 0]]).unwrap();
    let edge = mesh.mesh.edge_slot(0, 1).unwrap();
    assert!(matches!(
        mesh.locate_boundary_split(edge, XY::new(1.0, 0.5)),
        Ok(Located::Inside(_)),
    ));
}

#[test]
fn unrepresentable_circumcenter_is_an_explicit_midpoint_case() {
    let a = XY::new(0.0, 0.0);
    let b = XY::new(1.0, 0.0);
    let c = XY::new(0.5, f64::from_bits(1));

    assert_eq!(circumcenter(a, b, c).unwrap(), None);
    assert_eq!(longest_edge_midpoint(a, b, c), XY::new(0.5, 0.0));
}

#[test]
fn collinear_circumcenter_failure_is_not_a_midpoint() {
    let error = circumcenter(XY::new(0.0, 0.0), XY::new(1.0, 1.0), XY::new(2.0, 2.0)).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Voronoi(_))
    ));
}
