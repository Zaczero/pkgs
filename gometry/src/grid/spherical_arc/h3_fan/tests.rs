use super::*;

#[test]
fn h3_reflex_vertex_uses_adjacent_triangle_union() {
    for raw in [0x0810_1BFF_FFFF_FFFF_u64, 0x0830_136F_FFFF_FFFF_u64] {
        let cell = h3o::CellIndex::try_from(raw).unwrap();
        let arcs = h3_ordered_arcs(cell).expect("raw H3 arcs");
        let center = project_h3_seed(
            &cell
                .target_substrate_center_seed(cell.resolution())
                .expect("target substrate centre"),
        )
        .expect("certified centre");
        let vertex = arcs.get(6).expect("fixture reflex arc");
        let previous = arcs.get(5).expect("fixture preceding arc");
        let candidate = vertex
            .start
            .scale(Bound::exact(0.999).unwrap())
            .and_then(|value| {
                center
                    .scale(Bound::exact(0.0005).unwrap())
                    .and_then(|center| value.add(center))
            })
            .and_then(|value| {
                previous
                    .start
                    .scale(Bound::exact(0.0005).unwrap())
                    .and_then(|previous| value.add(previous))
            })
            .expect("finite reflex tangent proposal");
        let tangent = Vec3Ball {
            x: Ball::from_bound(candidate.x).unwrap(),
            y: Ball::from_bound(candidate.y).unwrap(),
            z: Ball::from_bound(candidate.z).unwrap(),
        };
        let PhysicalEndpointKey::CellVertex(key) = vertex.endpoints[0] else {
            panic!("fixture ordinal six is a structural H3 insertion");
        };
        let plan = h3_fan_plan(cell);
        assert_eq!(
            plan.side_at_vertex(key, tangent),
            FanSide::Open,
            "the previous fan triangle admits the real Class-III reflex motion"
        );
    }
}

#[test]
fn zero_straddling_fan_diagonal_never_promotes_a_strict_real_edge() {
    // The real cell edge is strictly inward, while the artificial center
    // diagonal contains zero.  Only the latter is ambiguous: treating it
    // as open would manufacture a source/cell contact from the fan's
    // triangulation rather than from a physical H3 boundary.
    let triangle = H3FanTriangle {
        real_edge: Vec3Bound::exact([1.0, 0.0, 0.0]).unwrap(),
        diagonal: Vec3Bound::exact([0.0, 1.0, 0.0]).unwrap(),
    };
    let tangent = Vec3Ball {
        x: Ball::exact(1.0).unwrap(),
        y: Ball::new(0.0, 1.0).unwrap(),
        z: Ball::exact(0.0).unwrap(),
    };
    assert_eq!(triangle_side(&triangle, tangent), FanSide::Uncertain);
}
