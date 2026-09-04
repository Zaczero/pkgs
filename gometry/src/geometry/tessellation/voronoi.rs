#![allow(
    clippy::option_if_let_else,
    clippy::unwrap_used,
    reason = "the compact deterministic selector maintains the option invariant in the adjacent match"
)]

use crate::error::Result;
use crate::geometry::tessellation::{CertifiedDelaunay, Site, exact};
use crate::geometry::{HashMap, HashMapExt as _};

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
    face_component: Vec<usize>,
    centers: Vec<exact::ExactPoint>,
}

impl DelaunayComplex {
    pub(super) fn component_of_face(&self, face: usize) -> usize {
        self.face_component[face]
    }

    pub(super) fn center(&self, component: usize) -> &exact::ExactPoint {
        &self.centers[component]
    }

    pub(super) const fn component_count(&self) -> usize {
        self.centers.len()
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
        if reverse == crate::geometry::tessellation::EMPTY || edge > reverse {
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

    // A certified face is already non-collinear.  Keeping its canonical
    // minimum triangle is enough both to name a component deterministically
    // and to define its exact circumcenter; collecting every component site
    // only recreated this state at a much higher allocation cost.
    let mut component_faces: HashMap<usize, ([usize; 3], usize)> = HashMap::new();
    for (face, triangle) in triangles.iter().enumerate() {
        let root = complex_root(&mut parents, face);
        let mut key = *triangle;
        key.sort_unstable();
        component_faces
            .entry(root)
            .and_modify(|current| {
                if key < current.0 {
                    *current = (key, face);
                }
            })
            .or_insert((key, face));
    }

    let mut roots: Vec<_> = component_faces.keys().copied().collect();
    roots.sort_unstable_by_key(|root| component_faces[root].0);
    let component_by_root: HashMap<_, _> = roots
        .iter()
        .enumerate()
        .map(|(component, &root)| (root, component))
        .collect();
    let mut centers = Vec::with_capacity(roots.len());
    for root in roots {
        let face = component_faces[&root].1;
        let [a, b, c] = triangles[face];
        centers.push(exact::circumcenter(
            sites[a].point.xy(),
            sites[b].point.xy(),
            sites[c].point.xy(),
        )?);
    }
    let face_component = (0..triangles.len())
        .map(|face| component_by_root[&complex_root(&mut parents, face)])
        .collect();
    Ok(DelaunayComplex {
        face_component,
        centers,
    })
}
