#![expect(
    clippy::option_if_let_else,
    clippy::unwrap_used,
    reason = "the compact deterministic selector maintains the option invariant in the adjacent match"
)]

use crate::error::Result;
use crate::geometry::tessellation::{CertifiedDelaunay, EMPTY, Site, exact};

pub(super) fn snap_sites(sites: &[Site], radius: f64) -> Vec<Site> {
    let ordered = |value: f64| {
        let bits = value.to_bits();
        if bits >> 63 == 0 {
            bits ^ (1_u64 << 63)
        } else {
            !bits
        }
    };
    let mut by_x: std::collections::BTreeMap<u64, Vec<Site>> = std::collections::BTreeMap::new();
    let mut snapped = Vec::with_capacity(sites.len());
    for site in sites {
        let point = site.point.xy();
        let mut best: Option<Site> = None;
        let min_x = ordered(point.x - radius);
        let max_x = ordered(point.x + radius);
        for entries in by_x.range(min_x..=max_x) {
            for &existing in entries.1 {
                let comparison = best.map(|closest| {
                    exact::squared_distance_cmp(point, existing.point.xy(), closest.point.xy())
                });
                if exact::distance_within(existing.point.xy(), point, radius)
                    && comparison.is_none_or(|order| {
                        order.is_lt() || (order.is_eq() && existing.id < best.unwrap().id)
                    })
                {
                    best = Some(existing);
                }
            }
        }
        let representative = if let Some(existing) = best {
            existing
        } else {
            by_x.entry(ordered(point.x)).or_default().push(*site);
            *site
        };
        snapped.push(representative);
    }
    snapped
}

/// The certified Delaunay complex with triangulation-only diagonals removed.
///
/// `face_component` is deliberately the only face-to-dual-vertex mapping:
/// every maximal fan joined by exact cocircular diagonals owns one exact
/// circumcenter, so a later embedding cannot accidentally materialize the
/// same geometric Voronoi vertex once per triangulation face.
#[derive(Debug)]
pub(super) struct DelaunayComplex {
    pub(super) face_component: Vec<usize>,
    pub(super) centers: Vec<exact::ExactPoint>,
}

impl DelaunayComplex {
    pub(super) fn component_of_face(&self, face: usize) -> usize {
        self.face_component[face]
    }

    pub(super) fn center(&self, component: usize) -> &exact::ExactPoint {
        &self.centers[component]
    }
}

const fn complex_root(parents: &mut [usize], mut node: usize) -> usize {
    while parents[node] != node {
        parents[node] = parents[parents[node]];
        node = parents[node];
    }
    node
}

/// Collapse every exact-cocircular connected face fan in the certified mesh.
/// The representative and its defining triple depend only on canonical site
/// ids, never on candidate face order or on a floating circumcenter.
pub(super) fn delaunay_complex(
    mesh: &CertifiedDelaunay,
    sites: &[Site],
) -> Result<DelaunayComplex> {
    let triangles = mesh.triangles();
    let mut parents: Vec<_> = (0..triangles.len()).collect();
    for (edge, &reverse) in mesh.halfedges().iter().enumerate() {
        if reverse == EMPTY || edge > reverse {
            continue;
        }
        let left = edge / 3;
        let right = reverse / 3;
        if mesh.edge_is_cocircular(edge) {
            let left = complex_root(&mut parents, left);
            let right = complex_root(&mut parents, right);
            if left != right {
                let (keep, merge) = (left.min(right), left.max(right));
                parents[merge] = keep;
            }
        }
    }

    // One component per union-find root. Face order is deterministic (the
    // exact engine inserts in Hilbert order over the canonical sites), and a
    // component's id never reaches the output: rings start at their rounded
    // lexicographic minimum and edges sort canonically. Any face of the fan
    // defines the exact circumcenter, since all of them are cocircular.
    let mut component_of_root = vec![usize::MAX; triangles.len()];
    let mut centers = Vec::new();
    for (face, &[a, b, c]) in triangles.iter().enumerate() {
        let root = complex_root(&mut parents, face);
        if component_of_root[root] == usize::MAX {
            component_of_root[root] = centers.len();
            centers.push(exact::circumcenter(
                sites[a].point.xy(),
                sites[b].point.xy(),
                sites[c].point.xy(),
            )?);
        }
    }
    // The union-find storage has the same length and lifetime as the final
    // face-component map. Compress every path before reusing it: once each
    // parent points directly at a root, replacing it with the component id
    // cannot change a later root lookup.
    for face in 0..triangles.len() {
        parents[face] = complex_root(&mut parents, face);
    }
    for face in 0..triangles.len() {
        parents[face] = component_of_root[parents[face]];
    }
    Ok(DelaunayComplex {
        face_component: parents,
        centers,
    })
}
