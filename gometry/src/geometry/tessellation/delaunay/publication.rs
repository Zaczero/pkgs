use super::{
    CdtRefinement, CertifiedDelaunay, ConstrainedSourcePlan, CoordinateAxes, EMPTY,
    ExpansionBudget, GENERATED_ITEM_LIMIT, GeographicFrame, GeometryErrorKind, HasM, HasZ,
    MOrdinate, MeshEdgeRole, MeshProducer, MeshSiteLocation, MeshUseId, Point, PointSelectionChart,
    PreparedMeshBoundaryUse, PreparedMeshFace, PreparedMeshPublication, PreparedMeshSite, Result,
    RetainedPointCoordinates, Ring, RingOwner, Site, ZOrdinate, cdt, certified_delaunay,
    certified_delaunay_for_retained_points, constrained_source_plan, effective_refinement,
    exact_delaunay, validate_refinement_request,
};
#[cfg(test)]
use super::{note_constrained_output_reserve, note_delaunay_output_reserve};
use crate::geometry::numerics::product::ExactSign;

pub(in crate::geometry::tessellation) fn prepare_retained_delaunay_publication(
    frame: GeographicFrame,
    source_points: &[Point],
    budget: &mut ExpansionBudget,
) -> Result<PreparedMeshPublication> {
    let Some(chart) = PointSelectionChart::try_from_points(frame, source_points)? else {
        return Ok(PreparedMeshPublication::new(
            MeshProducer::Delaunay,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    };
    let mut chart_sites = chart.sites_from_points(source_points)?;
    chart_sites.sort_unstable_by(|left, right| left.compare_chart_xy(*right));
    let sites = chart_sites
        .iter()
        .enumerate()
        .map(|(producer_ordinal, &site)| {
            PreparedMeshSite::source(
                producer_ordinal,
                site.source_ordinal(),
                *source_points
                    .get(site.source_ordinal())
                    .expect("retained source ordinal belongs to the source point inventory"),
                MeshSiteLocation::Retained(site),
            )
        })
        .collect::<Vec<_>>();
    let coordinates = RetainedPointCoordinates::new(frame, &chart_sites);
    let mesh = certified_delaunay_for_retained_points(frame, &chart_sites)?;
    prepare_delaunay_publication(&mesh, sites, &coordinates, budget)
}

pub(in crate::geometry::tessellation) fn prepare_raw_delaunay_publication(
    source_points: &[Point],
    budget: &mut ExpansionBudget,
) -> Result<PreparedMeshPublication> {
    let mut indexed_points = source_points
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<_>>();
    indexed_points.sort_unstable_by(|left, right| {
        left.1
            .x
            .total_cmp(&right.1.x)
            .then_with(|| left.1.y.total_cmp(&right.1.y))
    });
    let sites = indexed_points
        .iter()
        .enumerate()
        .map(|(producer_ordinal, &(occurrence, point))| {
            PreparedMeshSite::source(
                producer_ordinal,
                occurrence,
                point,
                MeshSiteLocation::Cartesian,
            )
        })
        .collect::<Vec<_>>();
    let points = indexed_points
        .iter()
        .map(|&(_, point)| point)
        .collect::<Vec<_>>();
    let provider_sites = indexed_points
        .iter()
        .enumerate()
        .map(|(id, &(source_ordinal, point))| Site::raw(id, point, source_ordinal))
        .collect::<Vec<_>>();
    let mesh = certified_delaunay(&provider_sites)?;
    let coordinates = |site| exact_delaunay::site_xy(&points, site);
    prepare_delaunay_publication(&mesh, sites, &coordinates, budget)
}

#[expect(
    clippy::indexing_slicing,
    reason = "canonical side maps and halfedge columns are emitted together by the same certified mesh"
)]
fn prepare_delaunay_publication<C: exact_delaunay::MeshCoordinates>(
    mesh: &CertifiedDelaunay,
    sites: Vec<PreparedMeshSite>,
    coordinates: &C,
    budget: &mut ExpansionBudget,
) -> Result<PreparedMeshPublication> {
    let canonical_faces = exact_delaunay::canonical_triangle_order(coordinates, mesh.triangles());
    let capacity = canonical_faces
        .len()
        .checked_mul(4)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    budget.check_additional(capacity)?;
    #[cfg(test)]
    note_delaunay_output_reserve();

    let mut points = Vec::with_capacity(capacity);
    let mut faces = Vec::with_capacity(canonical_faces.len());
    for canonical in canonical_faces {
        let start = points.len();
        let output_points = canonical.output_sites.map(|site| {
            sites
                .get(site)
                .expect("canonical site ordinal belongs to the prepared site table")
                .source_record()
        });
        let [a, b, c, closure] = selected_triangle_points(&output_points, [0, 1, 2], false);
        points.extend_from_slice(&[a, b, c, closure]);
        let end = points.len();
        let boundary_uses = std::array::from_fn(|output_side| {
            let selected_side = usize::from(canonical.output_to_selected[output_side]);
            let selected_slot = canonical
                .selected_face
                .wrapping_mul(3)
                .wrapping_add(selected_side);
            let reverse_slot = mesh.halfedges()[selected_slot];
            let reverse = (reverse_slot != EMPTY).then(|| {
                MeshUseId::new(
                    reverse_slot / 3,
                    u8::try_from(reverse_slot % 3)
                        .expect("halfedge side is one of the three triangle sides"),
                )
            });
            let selected_use = MeshUseId::new(
                canonical.selected_face,
                u8::try_from(selected_side).expect("triangle side fits u8"),
            );
            PreparedMeshBoundaryUse::new(
                u8::try_from(output_side).expect("triangle side fits u8"),
                selected_use,
                canonical.output_sites[output_side],
                canonical.output_sites[cdt::next_triangle_side(output_side)],
                reverse,
                canonical.reversed,
                mesh.edge_is_cocircular(selected_slot),
                MeshEdgeRole::Delaunay,
            )
        });
        faces.push(PreparedMeshFace::new(
            canonical.selected_face,
            canonical.selected_sites,
            canonical.output_sites,
            canonical.output_to_selected,
            canonical.reversed,
            start..end,
            &boundary_uses,
        ));
    }
    budget.add(capacity)?;
    Ok(PreparedMeshPublication::new(
        MeshProducer::Delaunay,
        sites,
        faces,
        points,
    ))
}

/// Build the constrained mesh plan shared by scalar and packed publication.
///
/// `rings` contains every shell and hole as a closed coordinate sequence.
/// Source sites retain the first full `Point` for each XY key, while each
/// nonzero constraint keeps those selected endpoint sites and its stable source
/// identity for exact refinement certificates.
pub(crate) fn prepare_constrained_triangle_plan<'a>(
    rings: impl Iterator<Item = (RingOwner, &'a Ring)>,
    area: f64,
    refinement: CdtRefinement,
    domain: crate::geometry::TopologyDomain,
    chart: Option<&PointSelectionChart>,
) -> Result<cdt::ConstrainedMeshPlan> {
    let mut ring_list = Vec::new();
    for ring in rings {
        ring_list.push(ring);
    }
    let rings = ring_list;
    let force_xy = refinement.active();
    let ConstrainedSourcePlan {
        mut vertices,
        sources,
        restrictions,
        source_rings,
        coordinates,
        chart_area,
    } = constrained_source_plan(&rings, domain, chart)?;
    let exact_chart_area = chart_area.as_ref();
    let refinement = effective_refinement(area, exact_chart_area, refinement);
    validate_refinement_request(area, exact_chart_area, refinement)?;
    let Some(mut cdt) = cdt::ConstrainedMesh::build_with_restrictions(
        &vertices,
        &sources,
        &restrictions,
        source_rings,
        coordinates,
    )?
    else {
        return Err(GeometryErrorKind::triangulation(
            "constrained triangulation needs three non-collinear vertices",
        ));
    };
    if let CdtRefinement::On {
        min_angle,
        max_area,
    } = refinement
    {
        // The refinement admission is for vertices inserted by refinement;
        // input constraint vertices are not generated output and must not
        // consume this operation's cap.
        let limit = GENERATED_ITEM_LIMIT;
        let complete = cdt.refine(
            &mut vertices,
            min_angle.map(crate::Positive::get),
            max_area.map(crate::Positive::get),
            limit,
        )?;
        if !complete {
            return Err(GeometryErrorKind::triangulation(format!(
                "constrained refinement could not satisfy the requested quality within the limit of {GENERATED_ITEM_LIMIT} generated vertices; relax min_angle/max_area"
            )));
        }
    }
    cdt.finish_plan(vertices, refinement, force_xy)
}

pub(crate) fn publish_constrained_plan(
    plan: &cdt::ConstrainedMeshPlan,
    budget: &mut ExpansionBudget,
) -> Result<Vec<Point>> {
    plan.validate_certificate()?;
    let faces = &plan.faces;
    let capacity = faces
        .len()
        .checked_mul(4)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    for &[a, b, c] in faces {
        plan.validate_refined_triangle([a, b, c], plan.refinement)?;
    }
    budget.check_additional(capacity)?;
    #[cfg(test)]
    note_constrained_output_reserve();
    let mut output = Vec::with_capacity(capacity);
    for &[a, b, c] in faces {
        let face_orientation = plan.triangle_orientation([a, b, c])?;
        let [a, b, c, _] = selected_triangle_points(&plan.vertices, [a, b, c], plan.force_xy);
        match face_orientation {
            ExactSign::Positive => output.extend_from_slice(&[a, b, c, a]),
            ExactSign::Negative | ExactSign::Zero => {
                return Err(GeometryErrorKind::triangulation(
                    "constrained triangulation produced a non-counter-clockwise face",
                ));
            },
        }
    }
    budget.add(capacity)?;
    Ok(output)
}

#[expect(
    clippy::indexing_slicing,
    reason = "the selected face contains only site IDs produced by the mesh over this point column"
)]
fn selected_triangle_points(points: &[Point], [a, b, c]: [usize; 3], force_xy: bool) -> [Point; 4] {
    let points = [points[a], points[b], points[c]];
    if force_xy {
        return [
            points[0].to_xy(),
            points[1].to_xy(),
            points[2].to_xy(),
            points[0].to_xy(),
        ];
    }
    let axes = CoordinateAxes::new(
        HasZ(points.iter().all(|point| point.axes.has_z())),
        HasM(points.iter().all(|point| point.axes.has_m())),
    );
    let point = |point: Point| {
        Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(axes.has_z().then(|| point.z().expect("shared Z axis"))),
            MOrdinate(axes.has_m().then(|| point.m().expect("shared M axis"))),
        )
    };
    [
        point(points[0]),
        point(points[1]),
        point(points[2]),
        point(points[0]),
    ]
}
