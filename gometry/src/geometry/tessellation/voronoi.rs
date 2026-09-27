use std::collections::{BTreeSet, VecDeque};

use crate::error::Result;
use crate::geometry::tessellation::{CertifiedDelaunay, EMPTY, Site, exact};

/// The sites that survive snapping at `radius`: a site within the radius of
/// an earlier survivor joins it, any other site survives. The sites arrive
/// sorted by x, so the active survivors are x-windowed and indexed by y.
#[expect(
    clippy::indexing_slicing,
    reason = "active and candidate ordinals come from this site's enumeration"
)]
pub(super) fn snap_sites(sites: &[Site], radius: f64) -> Result<Vec<Site>> {
    let ordered = |value: f64| {
        let bits = value.to_bits();
        if bits >> 63 == 0 {
            bits ^ (1_u64 << 63)
        } else {
            !bits
        }
    };
    let radius_bounds = exact::Interval::exact(radius);
    let mut survivors: Vec<Site> = Vec::new();
    let mut active: VecDeque<usize> = VecDeque::new();
    let mut y_index: BTreeSet<(u64, usize)> = BTreeSet::new();
    let mut candidates: Vec<usize> = Vec::new();
    for (ordinal, site) in sites.iter().enumerate() {
        let point = site.point.xy();
        let min_x = ordered(exact::Interval::exact(point.x).sub(radius_bounds).lo);
        let y_bounds = exact::Interval::exact(point.y);
        let min_y = ordered(y_bounds.sub(radius_bounds).lo);
        let max_y = ordered(y_bounds.add(radius_bounds).hi);
        while let Some(&oldest) = active.front() {
            if ordered(sites[oldest].point.x) >= min_x {
                break;
            }
            let expired = active
                .pop_front()
                .expect("the observed front remains present");
            let removed = y_index.remove(&(ordered(sites[expired].point.y), expired));
            debug_assert!(removed);
        }

        // Strict disk membership implies this closed outward y interval;
        // exact distance still owns the decision.
        candidates.clear();
        // The tree visits y order, but exact calls retain source/error order.
        candidates.extend(
            y_index
                .range((min_y, 0)..=(max_y, usize::MAX))
                .map(|&(_, candidate)| candidate),
        );
        candidates.sort_unstable();
        let mut snapped = false;
        for &candidate in &candidates {
            if exact::distance_within(sites[candidate].point.xy(), point, radius)? {
                snapped = true;
                break;
            }
        }
        if !snapped {
            survivors.push(*site);
            active.push_back(ordinal);
            y_index.insert((ordered(point.y), ordinal));
        }
    }
    Ok(survivors)
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
    let mut parents = Vec::with_capacity(triangles.len());
    parents.extend(0..triangles.len());
    let mut remaining_roots = triangles.len();
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
                // Distinct roots prove the remaining count cannot wrap here.
                remaining_roots = remaining_roots.wrapping_sub(1);
            }
        }
    }

    // One component per union-find root. Face order is deterministic (the
    // exact engine inserts in Hilbert order over the canonical sites), and a
    // component's id never reaches the output: rings start at their rounded
    // lexicographic minimum and edges sort canonically. Any face of the fan
    // defines the exact circumcenter, since all of them are cocircular.
    let mut component_of_root = vec![usize::MAX; triangles.len()];
    let mut centers = Vec::with_capacity(remaining_roots);
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
