use std::collections::{BTreeSet, VecDeque};

use crate::error::Result;
use crate::geometry::tessellation::{CertifiedDelaunay, EMPTY, Site, exact};

/// The sites that survive snapping at `radius`: a site within the radius of
/// an earlier survivor joins it, any other site survives. The sites arrive
/// sorted by x, so the active survivors are x-windowed and indexed by y.
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
    let mut survivor_for_site = vec![None; sites.len()];
    for (ordinal, site) in sites.iter().enumerate() {
        let [x_bounds, y_bounds] = site.exact_point.bounds();
        let min_x = x_bounds.sub(radius_bounds).lo;
        let min_y = ordered(y_bounds.sub(radius_bounds).lo.next_down().next_down());
        let max_y = ordered(y_bounds.add(radius_bounds).hi.next_up().next_up());
        while let Some(&oldest) = active.front() {
            if sites
                .get(oldest)
                .expect("active ordinals come from the source site enumeration")
                .exact_point
                .bounds()[0]
                .hi
                >= min_x
            {
                break;
            }
            let expired = active
                .pop_front()
                .expect("the observed front remains present");
            let removed = y_index.remove(&(
                ordered(
                    sites
                        .get(expired)
                        .expect("expired ordinals come from the active site queue")
                        .exact_point
                        .round_nearest_even()?
                        .y,
                ),
                expired,
            ));
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
        let mut snapped_to = None;
        for &candidate in &candidates {
            let candidate_site = sites
                .get(candidate)
                .expect("candidate ordinals come from the source site enumeration");
            let within = if candidate_site.retained_chart_site.is_none()
                && site.retained_chart_site.is_none()
            {
                exact::distance_within(candidate_site.point.xy(), site.point.xy(), radius)?
            } else {
                exact::distance_within_points(
                    &candidate_site.exact_point,
                    &site.exact_point,
                    radius,
                )?
            };
            if within {
                snapped_to = Some(candidate);
                break;
            }
        }
        if let Some(candidate) = snapped_to {
            let survivor = survivor_for_site
                .get(candidate)
                .copied()
                .flatten()
                .expect("snapping candidates are admitted survivors");
            let target: &mut Site = survivors
                .get_mut(survivor)
                .expect("survivor indices belong to the retained site table");
            target.snapped_sources.push(site.source_ordinal);
            target
                .snapped_sources
                .extend_from_slice(&site.quotient_aliases);
            target
                .snapped_sources
                .extend_from_slice(&site.snapped_sources);
            *survivor_for_site
                .get_mut(ordinal)
                .expect("site ordinal comes from the source enumeration") = Some(survivor);
        } else {
            let survivor = survivors.len();
            survivors.push(site.clone());
            *survivor_for_site
                .get_mut(ordinal)
                .expect("site ordinal comes from the source enumeration") = Some(survivor);
            active.push_back(ordinal);
            y_index.insert((ordered(site.exact_point.round_nearest_even()?.y), ordinal));
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
    pub(super) center_sites: Vec<[u32; 3]>,
}

impl DelaunayComplex {
    pub(super) fn component_of_face(&self, face: usize) -> usize {
        *self
            .face_component
            .get(face)
            .expect("face ordinal comes from the certified Delaunay mesh")
    }

    pub(super) fn center(&self, component: usize) -> &exact::ExactPoint {
        self.centers
            .get(component)
            .expect("component ordinal comes from the Delaunay complex")
    }
}

fn complex_root(parents: &mut [usize], mut node: usize) -> usize {
    loop {
        let parent = *parents
            .get(node)
            .expect("union-find node comes from the certified mesh");
        if parent == node {
            return node;
        }
        let grandparent = *parents
            .get(parent)
            .expect("union-find parent comes from the parent column");
        *parents
            .get_mut(node)
            .expect("union-find node comes from the certified mesh") = grandparent;
        node = grandparent;
    }
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
                *parents
                    .get_mut(merge)
                    .expect("merged root comes from the parent column") = keep;
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
    let mut center_sites = Vec::with_capacity(remaining_roots);
    for (face, &[a, b, c]) in triangles.iter().enumerate() {
        let root = complex_root(&mut parents, face);
        let component = component_of_root
            .get_mut(root)
            .expect("face root comes from the Delaunay parent column");
        if *component == usize::MAX {
            *component = centers.len();
            let [a_site, b_site, c_site] = [
                sites
                    .get(a)
                    .expect("triangle site comes from the canonical site set"),
                sites
                    .get(b)
                    .expect("triangle site comes from the canonical site set"),
                sites
                    .get(c)
                    .expect("triangle site comes from the canonical site set"),
            ];
            centers.push(
                if a_site.retained_chart_site.is_none()
                    && b_site.retained_chart_site.is_none()
                    && c_site.retained_chart_site.is_none()
                {
                    exact::circumcenter(a_site.point.xy(), b_site.point.xy(), c_site.point.xy())?
                } else {
                    exact::circumcenter_points(
                        &a_site.exact_point,
                        &b_site.exact_point,
                        &c_site.exact_point,
                    )?
                },
            );
            center_sites.push([
                u32::try_from(a).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                u32::try_from(b).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                u32::try_from(c).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            ]);
        }
    }
    // The union-find storage has the same length and lifetime as the final
    // face-component map. Compress every path before reusing it: once each
    // parent points directly at a root, replacing it with the component id
    // cannot change a later root lookup.
    for face in 0..triangles.len() {
        let root = complex_root(&mut parents, face);
        *parents
            .get_mut(face)
            .expect("face ordinal comes from the triangle iterator") = root;
    }
    for parent in &mut parents {
        *parent = *component_of_root
            .get(*parent)
            .expect("face root comes from the Delaunay parent column");
    }
    Ok(DelaunayComplex {
        face_component: parents,
        centers,
        center_sites,
    })
}
