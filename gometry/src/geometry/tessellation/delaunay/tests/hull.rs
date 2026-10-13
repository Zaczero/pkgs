use super::super::hull::delaunay_euler_complete;
use super::*;
use crate::geometry::{Orientation, orientation};

#[test]
fn hull_retains_collinear_boundary_sites_and_collinear_input_is_empty() {
    let boundary_sites = sites(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)]);
    let mesh = certified_delaunay(&boundary_sites).unwrap();
    let mut hull_sites = mesh.hull().to_vec();
    hull_sites.sort_unstable();
    assert_eq!(hull_sites.as_slice(), &[0, 1, 2, 3, 4]);
    let center = Point::new_raw_xy(1.0, 1.0).xy();
    for (&start, &end) in mesh.hull().iter().zip(mesh.hull().iter().cycle().skip(1)) {
        let start = boundary_sites
            .get(start)
            .expect("hull sites come from the input")
            .point
            .xy();
        let end = boundary_sites
            .get(end)
            .expect("hull sites come from the input")
            .point
            .xy();
        assert_eq!(
            exact::orient2d(start, end, center),
            ExactSign::Positive,
            "the hull cycle is counter-clockwise"
        );
    }

    let collinear = sites(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]);
    let mesh = certified_delaunay(&collinear).unwrap();
    assert!(mesh.triangles().is_empty());
    let mut hull_sites = mesh.hull().to_vec();
    hull_sites.sort_unstable();
    assert_eq!(hull_sites.as_slice(), &[0, 1, 2]);
}

#[test]
fn cocircular_state_is_reciprocal() {
    let sites = sites(&[(1.0, 0.0), (0.0, 1.0), (-1.0, 0.0), (0.0, -1.0)]);
    let mesh = certified_delaunay(&sites).unwrap();
    let mut cocircular_pairs = 0;
    for (slot, &reverse) in mesh.halfedges().iter().enumerate() {
        if reverse == EMPTY {
            continue;
        }
        assert_eq!(
            mesh.edge_is_cocircular(slot),
            mesh.edge_is_cocircular(reverse)
        );
        cocircular_pairs += usize::from(mesh.edge_is_cocircular(slot));
    }
    assert_eq!(cocircular_pairs, 2);
}

#[test]
fn public_topology_is_clockwise_and_euler_complete() {
    let points: Vec<Point> = [
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 3.0),
        (0.0, 3.0),
        (1.0, 1.0),
        (2.5, 2.0),
    ]
    .into_iter()
    .map(|(x, y)| Point::new_raw_xy(x, y))
    .collect();
    let topology = delaunay_triangulation(&points).unwrap();
    assert!(delaunay_euler_complete(points.len(), &topology));
    for &[a, b, c] in topology.triangles.as_chunks::<3>().0 {
        assert_eq!(
            orientation(points[a].xy(), points[b].xy(), points[c].xy()),
            Orientation::Clockwise
        );
    }
    for (slot, &twin) in topology.halfedges.iter().enumerate() {
        if twin != EMPTY {
            assert_eq!(topology.halfedges[twin], slot);
        }
    }
    assert_eq!(topology.hull, [0, 1, 2, 3]);
}
