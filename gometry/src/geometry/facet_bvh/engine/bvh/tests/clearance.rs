use super::{BvhNode, ClearanceFacetBvh, FacetBvh, finite_midpoint};
use crate::geometry::{CoordSeq, Point, PreparedLinework, source_dot_cmp};

fn collect_descendant_facets(tree: &ClearanceFacetBvh, node: usize, facets: &mut Vec<usize>) {
    match tree
        .tree
        .decode_node(u32::try_from(node).expect("clearance node IDs fit u32"))
    {
        BvhNode::Leaf(facet) => facets.push(facet),
        BvhNode::Internal(first_child) => {
            collect_descendant_facets(tree, first_child, facets);
            collect_descendant_facets(tree, first_child.wrapping_add(1), facets);
        },
    }
}

#[test]
fn facet_center_midpoints_stay_finite_at_coordinate_extremes() {
    let maximum = f64::MAX;
    assert_eq!(finite_midpoint(maximum, maximum), maximum);
    assert_eq!(finite_midpoint(-maximum, maximum), 0.0);
    assert_eq!(
        finite_midpoint(0.0, 2.0 * f64::from_bits(1)),
        f64::from_bits(1),
    );
}

#[test]
fn clearance_strip_supports_enclose_every_source_vertex() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let maximum = f64::MAX;
    let points = [
        Point::new_raw_xy(-maximum, -maximum),
        Point::new_raw_xy(maximum, maximum),
        Point::new_raw_xy(maximum, -maximum),
        Point::new_raw_xy(-maximum, maximum),
        Point::new_raw_xy(-(exact_power_of_two(600)), exact_power_of_two(600)),
        Point::new_raw_xy(exact_power_of_two(600), -(exact_power_of_two(600))),
        Point::new_raw_xy(-maximum, -maximum),
        Point::new_raw_xy(maximum, maximum),
        Point::new_raw_xy(maximum, -maximum),
        Point::new_raw_xy(-maximum, maximum),
        Point::new_raw_xy(-(exact_power_of_two(600)), exact_power_of_two(600)),
        Point::new_raw_xy(exact_power_of_two(600), -(exact_power_of_two(600))),
        Point::new_raw_xy(-maximum, -maximum),
        Point::new_raw_xy(maximum, maximum),
        Point::new_raw_xy(maximum, -maximum),
        Point::new_raw_xy(-maximum, maximum),
        Point::new_raw_xy(-(exact_power_of_two(600)), exact_power_of_two(600)),
    ];
    let first_chain =
        CoordSeq::try_from_points(points.get(..9).expect("first facet chain exists")).unwrap();
    let second_chain =
        CoordSeq::try_from_points(points.get(9..).expect("second facet chain exists")).unwrap();
    let linework = PreparedLinework::from_chains(vec![first_chain, second_chain]).unwrap();
    let clearance = FacetBvh::build_for_clearance(&linework)
        .unwrap()
        .expect("linework has facets");

    for (node, strip) in clearance.strips.iter().enumerate() {
        let low = linework
            .vertex_at_chain_local(strip.low_support.chain, strip.low_support.local)
            .xy();
        let high = linework
            .vertex_at_chain_local(strip.high_support.chain, strip.high_support.local)
            .xy();
        let mut facets = Vec::new();
        collect_descendant_facets(&clearance, node, &mut facets);
        for facet_index in facets {
            let facet = *linework
                .facets
                .get(facet_index)
                .expect("clearance leaf refers to prepared linework");
            for local in 0..=facet.segment_count as usize {
                let vertex_index = linework.facet_vertex_index(facet, local);
                let vertex = linework.vertex_at(vertex_index).xy();
                assert!(
                    !source_dot_cmp(strip.normal, low, vertex).unwrap().is_gt(),
                    "node {node} low support excludes source vertex {vertex_index}"
                );
                assert!(
                    !source_dot_cmp(strip.normal, vertex, high).unwrap().is_gt(),
                    "node {node} high support excludes source vertex {vertex_index}"
                );
            }
        }
    }
}
