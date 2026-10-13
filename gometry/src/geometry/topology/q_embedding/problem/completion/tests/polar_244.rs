    #[test]
    fn production_q_ring_role_check_rejects_a_complementary_source_drawing() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((0 10, 120 10, 240 10, 0 10))")?;
        let topology = topology(std::slice::from_ref(&source))?;
        let mut budget = EvaluationBudget::new("QC-R", 0)?;
        let plan =
            super::super::emission::plan::QEmissionPlan::build(&topology, OverlayOp::Union, &mut budget)?;
        let source_points = vec![None; topology.sources.points.len()];
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let mut drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
        let super::super::emission::plan::DrawnPolygons::Regions { rings, .. } = &mut drawing.polygons
        else {
            return Err(GeometryErrorKind::Invalid(
                "the cap did not produce a bounded source ring".into(),
            )
            .into());
        };
        let ring = rings
            .first_mut()
            .ok_or_else(|| GeometryErrorKind::Invalid("the cap source ring is missing".into()))?;
        if !matches!(ring.origin, super::super::emission::plan::DrawnAreaOrigin::Source) {
            return Err(GeometryErrorKind::Invalid(
                "the cap source ring lost its source authority".into(),
            )
            .into());
        }
        let mut complementary_points = ring
            .vertices
            .iter()
            .map(|vertex| vertex.point)
            .collect::<Vec<_>>();
        complementary_points.reverse();
        for (vertex, point) in ring.vertices.iter_mut().zip(complementary_points) {
            vertex.point = point;
        }

        match check_q_ring_roles(&topology, &drawing, &mut budget)? {
            QRingRoleCheck::Reject {
                failure: QRingRoleFailure::SourceChanged,
                ..
            } => {},
            QRingRoleCheck::Reject { .. } => {
                return Err(GeometryErrorKind::Invalid(
                    "the complementary source drawing failed for a non-role reason".into(),
                )
                .into());
            },
            QRingRoleCheck::Pass(roles) => {
                return Err(GeometryErrorKind::Invalid(
                    format!("QC-R accepted the complementary source role {roles:?}").into(),
                )
                .into());
            },
            QRingRoleCheck::NativeRequired(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "the complementary source drawing incorrectly required Native role assembly"
                        .into(),
                )
                .into());
            },
        }
        assert!(
            certify_nearest_ring_roles(&topology, &drawing, &mut budget).is_err(),
            "the production role certification must reject the complementary drawing"
        );
        Ok(())
    }
