use super::*;

#[test]
fn affine_parent_map_keeps_structure_and_endpoint_roles_explicit() {
    let general_zero_longitude = AffineParentMap {
        lambda0: Bound::exact(0.0).unwrap(),
        phi0: Bound::exact(0.0).unwrap(),
        dlambda: Bound::exact(0.0).unwrap(),
        dphi: Bound::exact(1.0).unwrap(),
        structure: AffineStructure::General,
    };
    let source = AffineSourceArc::from_parent_map(
        &general_zero_longitude,
        Bound::new(0.0, 1.0).unwrap(),
        [SourceEndpointRole::Partition, SourceEndpointRole::ParentEnd],
        [None, Some(PhysicalEndpointKey::NorthPole)],
        [AffineEndpointIdentity::default(); 2],
    )
    .unwrap();
    assert!(!source.is_vertical());
    assert!(!source.is_axis(AxisMeridian::Prime));
    assert_eq!(source.endpoint_pole(0), None);
    assert_eq!(
        source.endpoint_pole(1),
        Some(PhysicalEndpointKey::NorthPole)
    );

    let inconsistent_constant = AffineParentMap {
        dlambda: Bound::exact(1.0).unwrap(),
        structure: AffineStructure::ConstantLongitude {
            axis: Some(AxisMeridian::Prime),
        },
        ..general_zero_longitude
    };
    assert!(
        AffineSourceArc::from_parent_map(
            &inconsistent_constant,
            Bound::new(0.0, 1.0).unwrap(),
            [
                SourceEndpointRole::ParentStart,
                SourceEndpointRole::ParentEnd
            ],
            [None, None],
            [AffineEndpointIdentity::default(); 2],
        )
        .is_none()
    );
    assert!(
        AffineSourceArc::from_parent_map(
            &general_zero_longitude,
            Bound::new(0.0, 1.0).unwrap(),
            [SourceEndpointRole::Partition, SourceEndpointRole::ParentEnd],
            [Some(PhysicalEndpointKey::NorthPole), None],
            [AffineEndpointIdentity::default(); 2],
        )
        .is_none()
    );
}
