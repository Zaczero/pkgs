use super::{
    BvhCode, ClearanceFacetBvh, ClearanceStrip, ClearanceSupport, Facet, FacetBvh,
    PreparedLinework, clearance_strip_for_facets, union_aabb,
};

impl FacetBvh {
    /// Build over `linework`'s facets; `None` when it has none — or so many
    /// that a NODE index (`2F − 1` of them) could reach [`BvhCode::LEAF_BIT`] and a
    /// first-child code would read as a leaf (those astronomically large
    /// shapes keep the flat facet scans, which are index-width-agnostic).
    pub(crate) fn build(linework: &PreparedLinework) -> Option<Self> {
        let (mut tree, facet_aabbs, mut ids) = Self::build_parts(linework)?;
        let mut next = 1;
        tree.fill_node(0, &mut next, &facet_aabbs, &mut ids);
        Some(tree)
    }

    pub(crate) fn build_for_clearance(
        linework: &PreparedLinework,
    ) -> crate::error::Result<Option<ClearanceFacetBvh>> {
        let Some((mut tree, facet_aabbs, mut ids)) = Self::build_parts(linework) else {
            return Ok(None);
        };
        let mut strips = vec![
            ClearanceStrip {
                normal: [1.0, 0.0],
                low_support: ClearanceSupport { chain: 0, local: 0 },
                high_support: ClearanceSupport { chain: 0, local: 0 },
            };
            tree.aabbs.len()
        ];
        let mut next = 1;
        tree.fill_clearance_node(0, &mut next, &facet_aabbs, &mut ids, linework, &mut strips)?;
        debug_assert_eq!(next, tree.aabbs.len());
        Ok(Some(ClearanceFacetBvh {
            tree,
            strips: strips.into_boxed_slice(),
        }))
    }

    fn build_parts(linework: &PreparedLinework) -> Option<(Self, Vec<[f64; 4]>, Vec<u32>)> {
        let facet_count = u32::try_from(linework.facets.len()).ok()?;
        if facet_count == 0 || facet_count > BvhCode::LEAF_BIT / 2 {
            return None;
        }
        let facet_aabbs: Vec<[f64; 4]> = linework
            .facets
            .iter()
            .map(|&facet| linework.facet_aabb(facet))
            .collect();
        let ids: Vec<u32> = (0..facet_count).collect();
        let node_count = (facet_count as usize).wrapping_mul(2).wrapping_sub(1);
        let tree = Self {
            aabbs: vec![[0.0; 4]; node_count].into_boxed_slice(),
            codes: vec![0; node_count].into_boxed_slice(),
        };
        Some((tree, facet_aabbs, ids))
    }

    /// Median split of facet centers into the node slot `node`.
    #[expect(
        clippy::indexing_slicing,
        reason = "Facet ids are below facet_aabbs.len(); recursive node ids are builder-produced and below the allocated 2F-1 aabbs/codes lengths"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "facet_count <= LEAF_BIT/2 keeps 2F-1 in usize; next advances only across sibling pairs within the allocated node_count"
    )]
    fn fill_node(
        &mut self,
        node: usize,
        next: &mut usize,
        facet_aabbs: &[[f64; 4]],
        ids: &mut [u32],
    ) {
        if let [id] = ids {
            self.aabbs[node] = facet_aabbs[*id as usize];
            self.codes[node] = BvhCode::leaf(*id as usize).raw();
            return;
        }
        let aabb = partition_aabb(facet_aabbs, ids);
        let mid = partition_facets(aabb, facet_aabbs, ids, false);
        let (lower, upper) = ids.split_at_mut(mid);
        let first_child = *next;
        *next += 2;
        self.aabbs[node] = aabb;
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_node(first_child, next, facet_aabbs, lower);
        self.fill_node(first_child + 1, next, facet_aabbs, upper);
    }

    /// Spatial median split, preferring a tighter source-ordinal median where available.
    #[expect(
        clippy::indexing_slicing,
        reason = "Facet ids and recursive node ids are builder-produced and stay below the allocated bounds/codes"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "facet_count <= LEAF_BIT/2 keeps 2F-1 in usize; next advances only across allocated sibling pairs"
    )]
    fn fill_clearance_node(
        &mut self,
        node: usize,
        next: &mut usize,
        facet_aabbs: &[[f64; 4]],
        ids: &mut [u32],
        linework: &PreparedLinework,
        strips: &mut [ClearanceStrip],
    ) -> crate::error::Result<()> {
        let aabb = partition_aabb(facet_aabbs, ids);
        self.aabbs[node] = aabb;
        strips[node] = clearance_strip_for_facets(linework, ids)?;
        if let [id] = ids {
            self.codes[node] = BvhCode::leaf(*id as usize).raw();
            return Ok(());
        }

        let mid = partition_facets(aabb, facet_aabbs, ids, true);
        let (lower, upper) = ids.split_at_mut(mid);
        let first_child = *next;
        *next += 2;
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_clearance_node(first_child, next, facet_aabbs, lower, linework, strips)?;
        self.fill_clearance_node(first_child + 1, next, facet_aabbs, upper, linework, strips)
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "facet IDs were admitted from the same facet_aabbs slice"
)]
fn partition_aabb(facet_aabbs: &[[f64; 4]], ids: &[u32]) -> [f64; 4] {
    ids.iter()
        .map(|&id| facet_aabbs[id as usize])
        .reduce(union_aabb)
        .expect("partition contains at least one facet")
}

fn finite_span(lower: f64, upper: f64) -> Option<f64> {
    if !lower.is_finite() || !upper.is_finite() || lower > upper {
        return None;
    }
    let span = upper - lower;
    Some(if span.is_finite() {
        span
    } else {
        upper * 0.5 - lower * 0.5
    })
}

pub(super) fn finite_midpoint(lower: f64, upper: f64) -> f64 {
    if lower.is_finite()
        && upper.is_finite()
        && lower.is_sign_negative() == upper.is_sign_negative()
    {
        lower + (upper - lower) * 0.5
    } else {
        lower * 0.5 + upper * 0.5
    }
}

fn relative_span(child: f64, parent: f64) -> Option<f64> {
    if parent == 0.0 {
        (child == 0.0).then_some(0.0)
    } else {
        Some(child / parent)
    }
}

fn partition_cost(parent: [f64; 4], lower: [f64; 4], upper: [f64; 4]) -> Option<f64> {
    let [parent_min_x, parent_min_y, parent_max_x, parent_max_y] = parent;
    let [lower_min_x, lower_min_y, lower_max_x, lower_max_y] = lower;
    let [upper_min_x, upper_min_y, upper_max_x, upper_max_y] = upper;
    let parent_x = finite_span(parent_min_x, parent_max_x)?;
    let parent_y = finite_span(parent_min_y, parent_max_y)?;
    let lower_x = relative_span(finite_span(lower_min_x, lower_max_x)?, parent_x)?;
    let lower_y = relative_span(finite_span(lower_min_y, lower_max_y)?, parent_y)?;
    let upper_x = relative_span(finite_span(upper_min_x, upper_max_x)?, parent_x)?;
    let upper_y = relative_span(finite_span(upper_min_y, upper_max_y)?, parent_y)?;
    Some(lower_x + lower_y + upper_x + upper_y)
}

#[expect(
    clippy::indexing_slicing,
    reason = "facet IDs were admitted from the same facet_aabbs slice"
)]
fn partition_facets(
    aabb: [f64; 4],
    facet_aabbs: &[[f64; 4]],
    ids: &mut [u32],
    prefer_tighter_source_order: bool,
) -> usize {
    let [min_x, min_y, max_x, max_y] = aabb;
    let width = finite_span(min_x, max_x).unwrap_or(0.0);
    let height = finite_span(min_y, max_y).unwrap_or(0.0);
    let axis = usize::from(height > width);
    let center = |id: u32| {
        let [min_x, min_y, max_x, max_y] = facet_aabbs[id as usize];
        let (minimum, maximum) = if axis == 0 {
            (min_x, max_x)
        } else {
            (min_y, max_y)
        };
        finite_midpoint(minimum, maximum)
    };
    let mid = ids.len() / 2;
    ids.select_nth_unstable_by(mid, |&a, &b| {
        center(a).total_cmp(&center(b)).then_with(|| a.cmp(&b))
    });
    if !prefer_tighter_source_order {
        return mid;
    }
    // For clearance, source order is only a layout candidate: use it when its
    // normalized child extents tighten bounds more than the spatial median.
    let (lower, upper) = ids.split_at(mid);
    let spatial_cost = partition_cost(
        aabb,
        partition_aabb(facet_aabbs, lower),
        partition_aabb(facet_aabbs, upper),
    );
    ids.select_nth_unstable_by(mid, u32::cmp);
    let (lower, upper) = ids.split_at(mid);
    let source_cost = partition_cost(
        aabb,
        partition_aabb(facet_aabbs, lower),
        partition_aabb(facet_aabbs, upper),
    );
    if source_cost
        .zip(spatial_cost)
        .is_some_and(|(source, spatial)| source < spatial)
    {
        return mid;
    }
    ids.select_nth_unstable_by(mid, |&a, &b| {
        center(a).total_cmp(&center(b)).then_with(|| a.cmp(&b))
    });
    mid
}

pub(super) fn facet_for_leaf(linework: &PreparedLinework, index: usize) -> Facet {
    *linework
        .facets
        .get(index)
        .expect("BVH leaf facet index is within its prepared linework")
}
