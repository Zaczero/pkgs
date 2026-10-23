use super::super::exact::SourceRowId;
use super::super::q::ExactGeographicTopology;
use super::{GeographicCoverageFaceAssignment, analyze_geographic_coverage};
use crate::geographic::{GeographicFrame, ScalarGeographicView};
use crate::geometry::Shape;
use crate::geometry::coverage::CoverageOverlapRule;
use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::work::Unmetered;

fn topology(rows: &[Shape]) -> ExactGeographicTopology {
    let frame = GeographicFrame::crs84();
    ExactGeographicTopology::from_views(
        frame,
        rows.iter().map(|shape| {
            ScalarGeographicView::try_new(frame, shape).expect("geographic fixture admits")
        }),
    )
    .expect("Q topology builds")
}

fn face_rows(topology: &ExactGeographicTopology) -> Vec<Vec<usize>> {
    let mut owners = Vec::new();
    let mut work = Unmetered;
    topology
        .q
        .face_rows
        .for_each_face_occupants(&mut work, |face, rows| {
            let actual: Vec<_> = rows.iter().map(|row| row.0 as usize).collect();
            for row in 0..topology.sources.row_count {
                assert_eq!(
                    rows.binary_search(&SourceRowId(u32::try_from(row).expect("test row ID fits")))
                        .is_ok(),
                    topology.q.face_rows.contains(
                        face,
                        SourceRowId(u32::try_from(row).expect("test row ID fits")),
                    ),
                );
            }
            owners.push(actual);
            Ok(())
        })
        .expect("face occupancy sweep");
    owners.sort_unstable();
    owners
}

fn cleaning_assignments(
    rows: &[Shape],
    gap_width: f64,
    overlap_rule: CoverageOverlapRule,
) -> (Vec<Vec<usize>>, GeographicCoverageFaceAssignment) {
    let frame = GeographicFrame::crs84();
    let analysis = analyze_geographic_coverage(rows, frame, 0.0, "coverage_clean")
        .expect("geographic coverage admits");
    let face_rows = analysis.face_rows.clone();
    let source_rows = rows.iter().collect::<Vec<_>>();
    let assignments = crate::crs::with_ellipsoid_metric("OGC:CRS84", &source_rows, |metric| {
        let row_surface_areas = rows
            .iter()
            .map(|shape| crate::crs::chart_shape_area(metric, shape, frame))
            .collect::<crate::error::Result<Vec<_>>>()?;
        analysis.assign_cleaning_faces(
            Some(&row_surface_areas),
            gap_width,
            overlap_rule,
            Some(metric),
            &mut Unmetered,
        )
    })
    .expect("coverage face labels");
    (face_rows, assignments)
}

#[test]
fn sparse_face_row_sweep_distinguishes_disjoint_regions_and_complement() {
    let rows = [
        crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))").expect("first polygon"),
        crate::io::parse_wkt("POLYGON ((3 0, 4 0, 4 1, 3 1, 3 0))").expect("second polygon"),
    ];
    let owners = face_rows(&topology(&rows));
    assert!(owners.contains(&Vec::new()));
    assert!(owners.contains(&vec![0]));
    assert!(owners.contains(&vec![1]));
    assert!(owners.iter().all(|rows| rows.len() <= 1));
}

#[test]
fn sparse_face_row_sweep_retains_overlap_owners_together() {
    let rows = [
        crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))").expect("outer polygon"),
        crate::io::parse_wkt("POLYGON ((1 0.5, 3 0.5, 3 1.5, 1 1.5, 1 0.5))")
            .expect("overlapping polygon"),
    ];
    let owners = face_rows(&topology(&rows));
    assert!(owners.contains(&vec![0, 1]));
    assert!(owners.contains(&vec![0]));
    assert!(owners.contains(&vec![1]));
    assert!(owners.contains(&Vec::new()));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_face_labels_preserve_overlap_tie_rules() {
    let rows = [
        crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))").expect("larger polygon"),
        crate::io::parse_wkt("POLYGON ((1 0.5, 3 0.5, 3 1.5, 1 1.5, 1 0.5))")
            .expect("smaller polygon"),
    ];
    let rules = [
        (CoverageOverlapRule::MinIndex, 0),
        (CoverageOverlapRule::MaxArea, 0),
        (CoverageOverlapRule::MinArea, 1),
        (CoverageOverlapRule::LongestBorder, 1),
    ];
    for (rule, expected_row) in rules {
        let (face_rows, assignment) = cleaning_assignments(&rows, 0.0, rule);
        let overlap_face = face_rows
            .iter()
            .position(|owners| owners == &[0, 1])
            .expect("overlap has one worked Q face");
        assert_eq!(
            assignment.owners.get(overlap_face).copied().flatten(),
            Some(expected_row),
            "{rule:?}",
        );
    }

    let duplicate = rows[0].clone();
    let duplicate_rows = [rows[0].clone(), duplicate];
    let (face_rows, assignment) =
        cleaning_assignments(&duplicate_rows, 0.0, CoverageOverlapRule::LongestBorder);
    let overlap_face = face_rows
        .iter()
        .position(|owners| owners == &[0, 1])
        .expect("coincident rows share one Q face");
    assert_eq!(
        assignment.owners.get(overlap_face).copied().flatten(),
        Some(0),
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_face_sweep_and_selector_charge_the_supplied_work_account() {
    let rows =
        [crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))").expect("coverage polygon")];
    let analysis =
        analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_clean")
            .expect("geographic coverage admits");
    let row_areas = [rows[0].area()];

    let mut sweep_budget = EvaluationBudget::with_remaining_ticks_for_test("coverage_clean", 0, 0)
        .expect("empty test budget");
    let sweep_error = analysis
        .topology
        .q
        .face_rows
        .for_each_face_occupants(&mut sweep_budget, |_, _| Ok(()))
        .expect_err("face sweep consumes work");
    assert!(matches!(
        sweep_error.kind(),
        crate::error::ErrorKind::Geometry(
            crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "coverage_clean",
                ..
            }
        )
    ));

    let source_rows = rows.iter().collect::<Vec<_>>();
    let selector_error = crate::crs::with_ellipsoid_metric("OGC:CRS84", &source_rows, |metric| {
        let mut selector_budget =
            EvaluationBudget::with_remaining_ticks_for_test("coverage_clean", 0, 0)
                .expect("empty test budget");
        match analysis.assign_cleaning_faces(
            Some(&row_areas),
            1.0,
            CoverageOverlapRule::LongestBorder,
            Some(metric),
            &mut selector_budget,
        ) {
            Err(error) => Ok(error),
            Ok(_) => Err(crate::geometry::GeometryErrorKind::Invalid(
                "face metrics did not consume the selector work budget".into(),
            )
            .into()),
        }
    })
    .expect("metric-backed selection rejects an exhausted work budget");
    assert!(matches!(
        selector_error.kind(),
        crate::error::ErrorKind::Geometry(
            crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "coverage_clean",
                ..
            }
        )
    ));
}

#[test]
fn geographic_clean_labels_seam_overlap_without_absorbing_greenwich_rows() {
    let rows = [
        crate::io::parse_wkt("POLYGON ((170 0, -170 0, -170 10, 170 10, 170 0))")
            .expect("seam coverage row"),
        crate::io::parse_wkt("POLYGON ((175 2, -175 2, -175 8, 175 8, 175 2))")
            .expect("overlapping seam row"),
        crate::io::parse_wkt("POLYGON ((-5 2, 5 2, 5 8, -5 8, -5 2))").expect("Greenwich row"),
    ];
    let analysis =
        analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_clean")
            .expect("geographic source graph");
    let row_areas: Vec<_> = rows.iter().map(Shape::area).collect();
    let mut work = Unmetered;
    let assignment = analysis
        .assign_cleaning_faces(
            Some(&row_areas),
            0.0,
            CoverageOverlapRule::MinIndex,
            None::<&crate::crs::GeodesicMetric>,
            &mut work,
        )
        .expect("geographic row labels");

    let overlap = analysis
        .face_rows
        .iter()
        .position(|owners| owners == &[0, 1])
        .expect("Q retains the overlapping seam face");
    let greenwich = analysis
        .face_rows
        .iter()
        .position(|owners| owners == &[2])
        .expect("Q retains the disjoint Greenwich face");
    assert_eq!(assignment.owners.get(overlap).copied().flatten(), Some(0));
    assert_eq!(assignment.owners.get(greenwich).copied().flatten(), Some(2));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_face_labels_fill_only_the_enclosed_narrow_gap() {
    let rows: Vec<_> = (0..3)
        .flat_map(|x| {
            (0..3).filter(move |&y| x != 1 || y != 1).map(move |y| {
                let (x, y) = (f64::from(x), f64::from(y));
                crate::io::parse_wkt(&format!(
                    "POLYGON (({x} {y}, {} {y}, {} {}, {x} {}, {x} {y}))",
                    x + 1.0,
                    x + 1.0,
                    y + 1.0,
                    y + 1.0,
                ))
                .expect("coverage grid cell")
            })
        })
        .collect();
    let (face_rows, assignments) =
        cleaning_assignments(&rows, 1.0, CoverageOverlapRule::LongestBorder);
    let assigned_gaps: Vec<_> = assignments
        .gap_assignments
        .iter()
        .enumerate()
        .filter_map(|(face, &assigned)| assigned.then_some(face))
        .collect();
    assert_eq!(assigned_gaps.len(), 1);
    let gap_face = *assigned_gaps.first().expect("one selected gap");
    assert!(face_rows.get(gap_face).is_some_and(Vec::is_empty));
    // The southern horizontal border is longer in ellipsoidal metres, while
    // chart lengths tie and select the highest-index eastern neighbor.
    assert_eq!(assignments.owners.get(gap_face).copied().flatten(), Some(3));
    assert_eq!(
        face_rows
            .iter()
            .zip(&assignments.owners)
            .filter(|(rows, owner)| rows.is_empty() && owner.is_none())
            .count(),
        1,
        "the exterior complement is not a gap candidate",
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_face_labels_fill_a_seam_bordered_hole() {
    let holed = crate::io::parse_wkt(
        "POLYGON ((160 -10, -160 -10, -160 10, 160 10, 160 -10), \
             (178 -1, -178 -1, -178 1, 178 1, 178 -1))",
    )
    .expect("seam-bounded hole");
    let rows = [holed];
    let (face_rows, assignments) =
        cleaning_assignments(&rows, 2.0, CoverageOverlapRule::LongestBorder);
    let gap_face = assignments
        .gap_assignments
        .iter()
        .position(|&assigned| assigned)
        .expect("narrow seam-bordered hole is selected");
    assert!(face_rows.get(gap_face).is_some_and(Vec::is_empty));
    assert_eq!(assignments.owners.get(gap_face).copied().flatten(), Some(0));
    assert!(
        assignments.gap_assignments.len() == assignments.owners.len(),
        "face labels retain the global face inventory",
    );
}

#[rstest::rstest]
#[case::shell_latitude_70_0(70.0, 89.0)]
#[case::negative_shell_latitude_70_0(-70.0, -89.0)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_face_labels_keep_a_polar_hole_distinct_from_the_complement(
    #[case] shell_latitude: f64,
    #[case] hole_latitude: f64,
) {
    let holed = crate::io::parse_wkt(&format!(
        "POLYGON ((0 {shell_latitude}, 90 {shell_latitude}, \
             180 {shell_latitude}, 270 {shell_latitude}, 0 {shell_latitude}), \
             (0 {hole_latitude}, 90 {hole_latitude}, 180 {hole_latitude}, \
             270 {hole_latitude}, 0 {hole_latitude}))",
    ))
    .expect("polar shell and hole");
    let rows = [holed];
    let (face_rows, assignments) =
        cleaning_assignments(&rows, 3.0, CoverageOverlapRule::LongestBorder);
    let selected_gaps: Vec<_> = assignments
        .gap_assignments
        .iter()
        .enumerate()
        .filter_map(|(face, &assigned)| assigned.then_some(face))
        .collect();
    assert_eq!(selected_gaps.len(), 1);
    let hole_face = *selected_gaps.first().expect("polar hole is selected");
    assert_eq!(
        face_rows.first(),
        Some(&vec![0]),
        "global face zero is the occupied band, not a reserved exterior",
    );
    assert_eq!(assignments.owners.first(), Some(&Some(0)));
    assert!(face_rows.get(hole_face).is_some_and(Vec::is_empty));
    assert_eq!(
        assignments.owners.get(hole_face).copied().flatten(),
        Some(0)
    );
    assert_eq!(
        face_rows
            .iter()
            .zip(&assignments.owners)
            .filter(|(rows, owner)| rows.is_empty() && owner.is_none())
            .count(),
        1,
        "the complementary face remains unassigned",
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_face_zero_keeps_the_whole_sphere_row() {
    let whole_sphere = crate::io::parse_wkt(
        "POLYGON ((-180 -90, -90 -90, 0 -90, 90 -90, 180 -90, \
             180 90, 90 90, 0 90, -90 90, -180 90, -180 0, -180 -90))",
    )
    .expect("whole-sphere source ring");
    let rows = [whole_sphere];
    let (face_rows, assignments) =
        cleaning_assignments(&rows, 3.0, CoverageOverlapRule::LongestBorder);

    assert_eq!(face_rows, [vec![0]]);
    assert_eq!(assignments.owners, [Some(0)]);
    assert_eq!(assignments.gap_assignments, [false]);
}
