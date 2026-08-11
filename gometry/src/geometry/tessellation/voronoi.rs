#![allow(
    clippy::option_if_let_else,
    clippy::unwrap_used,
    reason = "the compact deterministic selector maintains the option invariant in the adjacent match"
)]

use crate::error::Result;
use crate::geometry::tessellation::{CertifiedDelaunay, Site, exact};
use crate::geometry::{GeometryErrorKind, HashMap, HashMapExt as _};

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
        if reverse == delaunator::EMPTY || edge > reverse {
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

    let mut sites_by_root: HashMap<usize, Vec<usize>> = HashMap::new();
    for (face, triangle) in triangles.iter().enumerate() {
        let root = complex_root(&mut parents, face);
        sites_by_root.entry(root).or_default().extend(triangle);
    }
    let mut roots: Vec<_> = sites_by_root.keys().copied().collect();
    for root in &roots {
        let component_sites = sites_by_root
            .get_mut(root)
            .expect("root was collected from this map");
        component_sites.sort_unstable();
        component_sites.dedup();
    }

    roots.sort_unstable_by(|&left, &right| sites_by_root[&left].cmp(&sites_by_root[&right]));
    let component_by_root: HashMap<_, _> = roots
        .iter()
        .enumerate()
        .map(|(component, &root)| (root, component))
        .collect();
    let mut centers = Vec::with_capacity(roots.len());
    for root in roots {
        let component_sites = &sites_by_root[&root];
        let mut defining = None;
        'triples: for first in 0..component_sites.len() {
            for second in first + 1..component_sites.len() {
                for third in second + 1..component_sites.len() {
                    let triple = [
                        component_sites[first],
                        component_sites[second],
                        component_sites[third],
                    ];
                    if exact::orient2d(
                        sites[triple[0]].point.xy(),
                        sites[triple[1]].point.xy(),
                        sites[triple[2]].point.xy(),
                    ) != exact::ExactSign::Zero
                    {
                        defining = Some(triple);
                        break 'triples;
                    }
                }
            }
        }
        let [a, b, c] = defining.ok_or_else(|| {
            GeometryErrorKind::voronoi("certified Delaunay component has no non-collinear face")
        })?;
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
