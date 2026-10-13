use h3o::Resolution;

use super::super::h3_boundary::h3_ordered_arcs;
use super::super::{AffineEndpointIdentity, AxisLatitude, AxisMeridian};
use super::*;

mod support;
use support::test_affine_source_arc;

#[test]
fn transverse_affine_edge_is_open_and_exterior_edge_is_none() {
    let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
    let transverse =
        test_affine_source_arc(40.0, -10.0, 40.0, 10.0).expect("finite source degrees");
    let exterior =
        test_affine_source_arc(-40.0, -10.0, -40.0, 10.0).expect("finite source degrees");

    assert_eq!(classify_arc_contact(&transverse, &arc), ArcContact::Open);
    assert_eq!(classify_arc_contact(&exterior, &arc), ArcContact::None);
}

#[test]
fn arc_contact_searches_the_source_piece_own_periodic_sheet() {
    let source = test_affine_source_arc(800.0, 0.0, 900.0, 0.0)
        .expect("a finite lifted equatorial source piece");
    let arc = OrderedMinorArc::from_test_points([0.0, 1.0, 0.0], [-1.0, 0.0, 0.0]);
    let (first, last) = source_arc_shift_range(&source, &arc)
        .expect("the finite source piece has a bounded periodic-copy range");
    assert!(
        first <= 2 && 2 <= last,
        "the 800..900 degree source piece needs the arc's +2 sheet"
    );
    assert_ne!(
        classify_arc_contact(&source, &arc),
        ArcContact::None,
        "the exact relation must not turn a remote periodic source contact into a negative"
    );
}

#[test]
fn source_endpoint_carrier_certifies_the_seam_contact_exclusion() {
    use crate::geometry::{CoordSeq, LineSeq, Point, Shape};
    use crate::grid::affine_source::{GeographicCoverageSource, SphericalGridTarget};

    let source = GeographicCoverageSource::new(
        &Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(170.0, -10.0).unwrap(),
                    Point::new(-170.0, 10.0).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
        SphericalGridTarget::H3(Resolution::Seven),
    )
    .unwrap();
    let cell = h3o::CellIndex::try_from(0x0877_EB57_26FF_FFFF_u64).unwrap();
    let source_arc = source.authority_pieces().unwrap().next().unwrap().arc();
    let endpoint = Bound::exact(source_arc.domain.hi).unwrap();
    assert_eq!(
        source_arc.endpoint_identity_at(endpoint),
        Some(AffineEndpointIdentity {
            longitude: Some(AxisMeridian::Antimeridian),
            latitude: Some(AxisLatitude::Equator),
        }),
        "the exact rational seam split, not its rounded enclosure, carries both axes"
    );
    let arcs = h3_ordered_arcs(cell).unwrap();
    // These are the two seam-side pieces whose overlap terminates at the
    // source parent endpoint.  Its outward longitude `hi` is not a new
    // exact longitude on the parent map, so direct inverse evaluation is
    // deliberately unavailable; the endpoint carrier supplies the sign.
    for (index, shift) in [(1, 0), (5, 1)] {
        let arc = arcs
            .get(index)
            .unwrap()
            .with_longitude_shift(shift)
            .unwrap();
        let LongitudeOverlap::Contact { outer, .. } = source_arc.longitude_span().unwrap().overlap(
            arc.longitude
                .and_then(DirectedLongitudeSpan::envelope)
                .unwrap(),
        ) else {
            panic!("fixture must enter the certified seam overlap");
        };
        assert!(
            plane_at_longitude(&source_arc, &arc, outer.hi).is_none(),
            "the rounded enclosure endpoint is intentionally not authority"
        );
        let endpoint_plane = plane_at_overlap_longitude(&source_arc, &arc, outer.hi);
        assert!(
            endpoint_plane
                .is_some_and(|value| value.strictly_positive() || value.strictly_negative()),
            "the retained parent endpoint proves the missing strict sign"
        );
        assert_eq!(
            classify_general_contact(&source_arc, &arc),
            ArcContact::None,
            "the endpoint carrier excludes the non-contacting seam cell"
        );
    }
}

#[test]
fn contact_order_is_fail_open() {
    assert_eq!(
        ArcContact::None.combine(ArcContact::ClosedOnly),
        ArcContact::ClosedOnly
    );
    assert_eq!(
        ArcContact::ClosedOnly.combine(ArcContact::Uncertain),
        ArcContact::Uncertain
    );
    assert_eq!(
        ArcContact::Uncertain.combine(ArcContact::Open),
        ArcContact::Open
    );
}

#[test]
fn general_relation_tries_the_three_certified_seam_shifts() {
    let radians = |degrees: f64| degrees.to_radians();
    let arc =
        OrderedMinorArc::from_test_points([radians(170.0).cos(), radians(170.0).sin(), 0.0], [
            radians(-170.0).cos(),
            radians(-170.0).sin(),
            0.0,
        ]);
    let source = test_affine_source_arc(180.0, -1.0, 180.0, 1.0).unwrap();
    assert_eq!(
        classify_arc_contact(&source, &arc),
        ArcContact::Open,
        "the +2pi copy is the only inner longitude overlap"
    );
}

#[test]
fn zero_length_source_on_a_general_arc_is_uncertain() {
    let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
    let point = test_affine_source_arc(45.0, 0.0, 45.0, 0.0).unwrap();
    assert_eq!(classify_arc_contact(&point, &arc), ArcContact::Uncertain);
}

#[test]
fn tangent_and_unresolved_plane_fail_open() {
    let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
    assert_eq!(
        arc.kind,
        ExactArcKind::General,
        "a strictly signed n.z is General even on the equatorial plane"
    );
    let tangent = test_affine_source_arc(20.0, 0.0, 70.0, 10.0).expect("finite source degrees");
    assert_eq!(classify_arc_contact(&tangent, &arc), ArcContact::Uncertain);

    let unresolved = OrderedMinorArc::from_test_points([0.0, 0.0, 1.0], [1.0, 0.0, 0.0]);
    assert_eq!(unresolved.kind, ExactArcKind::UnresolvedMeridian);
    assert_eq!(
        classify_arc_contact(&tangent, &unresolved),
        ArcContact::Uncertain
    );
}

#[test]
fn finite_extrema_certificate_controls_general_negative() {
    let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 1.0]);
    let source = test_affine_source_arc(30.0, 40.0, 60.0, 45.0).unwrap();
    let lambda = source.lambda_at(source.domain).unwrap();
    assert_eq!(
        certify_derivative_sign(&source, &arc, lambda),
        Some(CertSign::Negative),
        "this reaches B4's all-box strict derivative proof"
    );
    assert_eq!(
        classify_arc_contact(&source, &arc),
        ArcContact::None,
        "the generic branch consumes the B4 certificate for a negative"
    );

    let too_wide = test_affine_source_arc(-100.0, 40.0, 100.0, 45.0).unwrap();
    assert_eq!(
        certify_derivative_sign(
            &too_wide,
            &arc,
            too_wide.lambda_at(too_wide.domain).unwrap()
        ),
        None,
        "a source span beyond the one-minor-arc precondition cannot certify D"
    );
}
