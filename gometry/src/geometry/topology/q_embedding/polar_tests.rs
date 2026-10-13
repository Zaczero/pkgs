use super::*;
use crate::geometry::{CoordinateAxes, OverlayOp, Shape};

fn topology_in_frame(shapes: &[Shape], frame: GeographicFrame) -> Result<ExactGeographicTopology> {
    let inputs = crate::geographic::GeographicInputs::try_from_shapes(frame, shapes.iter())
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())
}

fn ring(points: &[(f64, f64)]) -> Result<Ring> {
    let points = points
        .iter()
        .map(|&(x, y)| Point::new_raw_xy(x, y))
        .collect::<Vec<_>>();
    Ring::try_from_closed(CoordSeq::try_from_points(&points)?)
}

fn polygon(points: &[(f64, f64)]) -> Result<Shape> {
    Ok(Shape::Polygon(crate::geometry::Polygon::new(
        ring(points)?,
        Vec::new(),
    )))
}

fn winding_pole_cap(reverse: bool) -> Result<Shape> {
    let mut boundary = vec![
        (0.0, 30.0),
        (90.0, 30.0),
        (180.0, 30.0),
        (-90.0, 30.0),
        (0.0, 30.0),
    ];
    if reverse {
        boundary.reverse();
    }
    polygon(&boundary)
}

#[derive(Debug)]
struct PolarCapture {
    roles: RoleCapture,
    selected_regions: Vec<(usize, bool, bool)>,
    pole_sectors: Vec<(usize, ChartPole, bool)>,
    cycles: Vec<(usize, bool, i64)>,
    rounded: bool,
    seal: SealCapture,
}

#[derive(Debug)]
enum RoleCapture {
    Admitted { unclassified_cycles: usize },
    Rejected(String),
}

#[derive(Debug)]
enum SealCapture {
    NotAttempted,
    Native,
    SourceOnly,
    Exhausted,
    Failed(String),
}

impl PolarCapture {
    fn unclassified_cycles(&self) -> std::result::Result<usize, &str> {
        match &self.roles {
            RoleCapture::Admitted {
                unclassified_cycles,
            } => Ok(*unclassified_cycles),
            RoleCapture::Rejected(error) => Err(error),
        }
    }

    fn native(&self) -> std::result::Result<(), &str> {
        match &self.seal {
            SealCapture::Native => Ok(()),
            SealCapture::Failed(error) => Err(error),
            SealCapture::NotAttempted => Err("role check rejected the drawing"),
            SealCapture::SourceOnly => Err("sealing selected the source drawing"),
            SealCapture::Exhausted => Err("native cut search exhausted its candidates"),
        }
    }

    fn source_only(&self) -> bool {
        matches!(&self.seal, SealCapture::SourceOnly)
    }
}

fn capture_native_seal(
    shapes: &[Shape],
    operation: OverlayOp,
    frame: GeographicFrame,
) -> Result<PolarCapture> {
    let topology = topology_in_frame(shapes, frame)?;
    let mut budget = EvaluationBudget::new(operation.name(), 0)?;
    let plan = QEmissionPlan::build(&topology, operation, &mut budget)?;
    let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
    let source_points = vec![None; topology.q.nodes.len()];
    let publication =
        plan.fragment_recipes
            .publish(&topology, &nearest, operation.name(), &mut budget)?;
    let emission = plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
    let drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
    let live_keys = drawing.live_node_keys(frame, &mut budget)?;
    let rounded = nearest.has_inexact_nodes_with_work(
        &topology,
        live_keys.iter().map(|(_, node)| *node),
        &mut budget,
    )?;
    let roles = match check_q_ring_roles(&topology, &drawing, &mut budget) {
        Ok(QRingRoleCheck::Pass(_)) => RoleCapture::Admitted {
            unclassified_cycles: 0,
        },
        Ok(QRingRoleCheck::NativeRequired(roles)) => RoleCapture::Admitted {
            unclassified_cycles: roles
                .iter()
                .filter(|role| matches!(role, QRingRoleStatus::Unclassified))
                .count(),
        },
        Ok(QRingRoleCheck::Reject { .. }) => {
            RoleCapture::Rejected("selected drawing role was rejected".to_owned())
        },
        Err(error) => RoleCapture::Rejected(error.to_string()),
    };
    let selected_regions = plan
        .selected_regions
        .iter()
        .map(|region| {
            (
                region.root,
                region.poles.includes(ChartPole::North),
                region.poles.includes(ChartPole::South),
            )
        })
        .collect();
    let selected_poles =
        native_selected_pole_sectors(&topology.q, &plan.selected_face_roots, &mut budget)?;
    let closures = native_pole_closure_regions(&selected_poles.sectors, &mut budget)?;
    let pole_sectors = selected_poles
        .sectors
        .iter()
        .map(|selected| {
            let pole = match selected.sector {
                super::super::q::PoleSector::Fan { pole, .. }
                | super::super::q::PoleSector::Isolated { pole, .. }
                | super::super::q::PoleSector::Terminal { pole, .. } => pole,
            };
            (
                selected.region,
                pole,
                closures.contains(&(selected.region, pole)),
            )
        })
        .collect();
    let cycles = match &drawing.polygons {
        DrawnPolygons::WholeSphere(_) => Vec::new(),
        DrawnPolygons::Regions { rings, .. } => rings
            .iter()
            .map(|ring| {
                let (chart, ..) = assigned_emitted_ring(&topology, &ring.vertices, &mut budget)?;
                let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&chart))
                    .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
                Ok((
                    ring.region,
                    matches!(ring.origin, DrawnAreaOrigin::Source),
                    summary.chart_turn,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    };
    if matches!(&roles, RoleCapture::Rejected(_)) {
        return Ok(PolarCapture {
            roles,
            selected_regions,
            pole_sectors,
            cycles,
            rounded,
            seal: SealCapture::NotAttempted,
        });
    }
    let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
    let certification = QEmbeddingCertification::<crate::crs::GeodesicMetric> {
        topology: &topology,
        emission: &emission,
        nearest: &nearest,
        ordinates: &ordinates,
        declared_axes: CoordinateAxes::XY,
        strict_axes: false,
        operation: operation.name(),
        metric_access: None,
    };
    let seal =
        match certify_and_search_q_embedding(&certification, &live_keys, drawing, &mut budget) {
            Err(error) => SealCapture::Failed(error.to_string()),
            Ok(SearchOutcome::Exhausted) => SealCapture::Exhausted,
            Ok(SearchOutcome::Pass(pass)) => match pass.output.sealed {
                super::super::emission::SealedQDrawing::Native(_) => SealCapture::Native,
                super::super::emission::SealedQDrawing::Source(_) => SealCapture::SourceOnly,
            },
        };
    Ok(PolarCapture {
        roles,
        selected_regions,
        pole_sectors,
        cycles,
        rounded,
        seal,
    })
}

fn t2_notched_inputs(frame: GeographicFrame) -> Result<[Shape; 2]> {
    let half = frame.longitude().half_period();
    let pole = frame.pole();
    let source = polygon(&[
        (-half, 0.125 * pole),
        (-half / 2.0, 0.125 * pole),
        (-half / 16.0, 0.125 * pole),
        (-half / 16.0, -0.125 * pole),
        (0.0, -0.125 * pole),
        (half / 16.0, -0.125 * pole),
        (half / 16.0, 0.125 * pole),
        (half / 2.0, 0.125 * pole),
        (half, 0.125 * pole),
        (half, pole),
        (half / 2.0, pole),
        (0.0, pole),
        (-half / 2.0, pole),
        (-half, pole),
        (-half, 0.125 * pole),
    ])?;
    let band = polygon(&[
        (-half, -0.25 * pole),
        (-half / 2.0, -0.25 * pole),
        (0.0, -0.25 * pole),
        (half / 2.0, -0.25 * pole),
        (-half, -0.25 * pole),
        (-half, 0.25 * pole),
        (half / 2.0, 0.25 * pole),
        (0.0, 0.25 * pole),
        (-half / 2.0, 0.25 * pole),
        (-half, 0.25 * pole),
        (-half, -0.25 * pole),
    ])?;
    Ok([source, band])
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses assertions as its behavioral oracle"
)]
fn t2_notched_selected_region_reaches_native_assembly() -> Result<()> {
    let [source, band] = t2_notched_inputs(GeographicFrame::crs84())?;
    let capture = capture_native_seal(
        &[source, band],
        OverlayOp::Intersection,
        GeographicFrame::crs84(),
    )?;

    assert_eq!(capture.unclassified_cycles(), Ok(1), "{capture:?}");
    assert!(
        capture
            .selected_regions
            .iter()
            .all(|(_, north, south)| !north && !south),
        "{capture:?}"
    );
    assert!(capture.pole_sectors.is_empty(), "{capture:?}");
    assert_eq!(capture.cycles.len(), 2, "{capture:?}");
    assert!(
        capture
            .cycles
            .iter()
            .all(|(_, source, turn)| !source && *turn != 0),
        "{capture:?}"
    );
    assert!(!capture.rounded, "{capture:?}");
    assert_eq!(capture.native(), Ok(()), "{capture:?}");
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses assertions as its behavioral oracle"
)]
fn t2_bounded_winding_exclusion_and_finite_hole_keep_the_same_native_region() -> Result<()> {
    let frame = GeographicFrame::crs84();
    let [shell, _] = t2_notched_inputs(frame)?;
    let finite_exclusion = polygon(&[
        (20.0, 10.0),
        (30.0, 10.0),
        (30.0, 15.0),
        (20.0, 15.0),
        (20.0, 10.0),
    ])?;
    for reverse_winding_hole in [false, true] {
        let winding_exclusion = winding_pole_cap(reverse_winding_hole)?;
        let capture = capture_native_seal(
            &[shell.clone(), winding_exclusion, finite_exclusion.clone()],
            OverlayOp::Difference,
            frame,
        )?;
        let generated_regions: Vec<_> = capture
            .cycles
            .iter()
            .filter_map(|(region, source, turn)| (!source && *turn != 0).then_some(*region))
            .collect();
        assert!(
            capture.unclassified_cycles().is_ok_and(|count| count >= 1),
            "{capture:?}"
        );
        assert!(
            capture
                .selected_regions
                .iter()
                .all(|(_, north, south)| !north && !south),
            "{capture:?}"
        );
        assert!(capture.pole_sectors.is_empty(), "{capture:?}");
        assert!(!generated_regions.is_empty(), "{capture:?}");
        assert!(
            generated_regions
                .first()
                .is_some_and(|first| generated_regions.iter().all(|region| region == first)),
            "{capture:?}"
        );
        assert!(
            capture
                .selected_regions
                .iter()
                .any(|(region, ..)| generated_regions.first() == Some(region)),
            "{capture:?}"
        );
        assert!(!capture.rounded, "{capture:?}");
        assert_eq!(capture.native(), Ok(()), "{capture:?}");
    }
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses assertions as its behavioral oracle"
)]
fn t2_two_ambiguous_selected_cycles_reach_native_assembly() -> Result<()> {
    let lower = polygon(&[
        (-180.0, -20.0),
        (-90.0, 10.0),
        (0.0, -20.0),
        (90.0, 10.0),
        (180.0, -20.0),
        (180.0, 90.0),
        (90.0, 90.0),
        (0.0, 90.0),
        (-90.0, 90.0),
        (-180.0, 90.0),
        (-180.0, -20.0),
    ])?;
    let upper = polygon(&[
        (-180.0, -15.0),
        (-90.0, 15.0),
        (0.0, -15.0),
        (90.0, 15.0),
        (180.0, -15.0),
        (180.0, 90.0),
        (90.0, 90.0),
        (0.0, 90.0),
        (-90.0, 90.0),
        (-180.0, 90.0),
        (-180.0, -15.0),
    ])?;

    let capture = capture_native_seal(
        &[lower, upper],
        OverlayOp::Difference,
        GeographicFrame::crs84(),
    )?;
    assert_eq!(capture.unclassified_cycles(), Ok(2), "{capture:?}");
    assert!(
        capture
            .selected_regions
            .iter()
            .all(|(_, north, south)| !north && !south),
        "{capture:?}"
    );
    assert!(capture.pole_sectors.is_empty(), "{capture:?}");
    assert_eq!(capture.cycles.len(), 2, "{capture:?}");
    assert!(
        capture
            .cycles
            .iter()
            .all(|(_, source, turn)| !source && *turn != 0),
        "{capture:?}"
    );
    assert!(!capture.rounded, "{capture:?}");
    assert_eq!(capture.native(), Ok(()), "{capture:?}");
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses assertions as its behavioral oracle"
)]
fn t2_generated_polar_cap_closure_reaches_native_without_vertex_rounding() -> Result<()> {
    let connector = polygon(&[
        (-10.0, -20.0),
        (10.0, -20.0),
        (10.0, 10.0),
        (-10.0, 10.0),
        (-10.0, -20.0),
    ])?;
    for latitude in [-5.0, 5.0] {
        let cap = polygon(&[
            (-180.0, latitude),
            (-90.0, latitude),
            (0.0, latitude),
            (90.0, latitude),
            (180.0, latitude),
            (-180.0, latitude),
        ])?;
        let capture = capture_native_seal(
            &[cap, connector.clone()],
            OverlayOp::Union,
            GeographicFrame::crs84(),
        )?;
        assert!(
            capture
                .selected_regions
                .iter()
                .any(|(_, north, south)| if latitude < 0.0 {
                    !north && *south
                } else {
                    *north && !south
                }),
            "latitude={latitude}: {capture:?}"
        );
        let selected_pole = if latitude < 0.0 {
            ChartPole::South
        } else {
            ChartPole::North
        };
        assert!(
            capture
                .pole_sectors
                .iter()
                .any(|(_, pole, closure)| *pole == selected_pole && *closure),
            "latitude={latitude}: {capture:?}"
        );
        assert_eq!(
            capture.unclassified_cycles(),
            Ok(1),
            "latitude={latitude}: {capture:?}"
        );
        assert!(!capture.rounded, "latitude={latitude}: {capture:?}");
        assert!(
            capture.native().is_ok(),
            "the generated boundary must reach selected-region closure at {latitude}: {capture:?}"
        );
    }
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses assertions as its behavioral oracle"
)]
fn t2_split_cap_reuse_preserves_the_native_pole_fan() -> Result<()> {
    let frame = GeographicFrame::crs84();
    let cap = Shape::Polygon(crate::geometry::Polygon::new(
        ring(&[
            (-180.0, -5.0),
            (-90.0, -5.0),
            (0.0, -5.0),
            (90.0, -5.0),
            (180.0, -5.0),
            (-180.0, -5.0),
        ])?,
        vec![ring(&[
            (20.0, -55.0),
            (30.0, -55.0),
            (30.0, -45.0),
            (20.0, -45.0),
            (20.0, -55.0),
        ])?],
    ));
    let connector = polygon(&[
        (-10.0, -20.0),
        (10.0, -20.0),
        (10.0, 10.0),
        (-10.0, 10.0),
        (-10.0, -20.0),
    ])?;
    let output = crate::geometry::overlay_all(
        &[&cap, &connector],
        crate::geometry::TopologyDomain::Geographic(frame),
        OverlayOp::Union,
        crate::geometry::Strictness::Strict,
    )?;
    let split = output.split_antimeridian()?;
    let capture =
        capture_native_seal(&[split.clone(), connector.clone()], OverlayOp::Union, frame)?;
    assert_eq!(capture.unclassified_cycles(), Ok(1), "{capture:?}");
    assert!(
        capture
            .selected_regions
            .iter()
            .any(|(_, north, south)| !north && *south),
        "{capture:?}"
    );
    assert!(!capture.rounded, "{capture:?}");
    assert_eq!(capture.native(), Ok(()), "{capture:?}");
    let reused = crate::geometry::overlay_all(
        &[&split, &connector],
        crate::geometry::TopologyDomain::Geographic(frame),
        OverlayOp::Union,
        crate::geometry::Strictness::Strict,
    )?;
    let reused_topology = topology_in_frame(&[reused], frame)?;
    assert!(!reused_topology.sources.rings.is_empty());
    assert!(
        reused_topology
            .sources
            .rings
            .iter()
            .all(|ring| ring.summary.chart_turn == 0),
        "reused output has a winding ring"
    );
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses assertions as its behavioral oracle"
)]
fn t2_unchanged_exact_seam_difference_remains_source_only() -> Result<()> {
    let source = crate::io::parse_wkt("POLYGON ((170 0, -170 0, -170 20, 170 20, 170 0))")?;
    let cutter = crate::io::parse_wkt("POLYGON ((175 10, -175 10, -175 30, 175 30, 175 10))")?;
    let capture = capture_native_seal(
        &[source, cutter],
        OverlayOp::Difference,
        GeographicFrame::crs84(),
    )?;

    assert_eq!(capture.unclassified_cycles(), Ok(0), "{capture:?}");
    assert!(!capture.rounded, "{capture:?}");
    assert!(capture.source_only(), "{capture:?}");
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses assertions as its behavioral oracle"
)]
fn t2_selected_region_assembly_without_rounding_uses_native_units() -> Result<()> {
    let radians = crate::geographic::AngularFactor::try_new("radian", 1.0)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let grads = crate::geographic::AngularFactor::try_new("grad", std::f64::consts::PI / 200.0)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let frames = [
        GeographicFrame::crs84(),
        GeographicFrame::try_new(radians, radians, 0.0)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?,
        GeographicFrame::try_new(grads, grads, 0.0)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?,
    ];
    for frame in frames {
        let [source, band] = t2_notched_inputs(frame)?;
        let capture = capture_native_seal(&[source, band], OverlayOp::Intersection, frame)?;
        assert!(!capture.rounded, "frame={frame:?}: {capture:?}");
        assert_eq!(
            capture.unclassified_cycles(),
            Ok(1),
            "frame={frame:?}: {capture:?}"
        );
        assert!(
            capture.native().is_ok(),
            "selected chart assembly was skipped for {frame:?}: {capture:?}"
        );
    }
    Ok(())
}
