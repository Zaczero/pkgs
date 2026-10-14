use super::earcut::{EarcutCornerId, EarcutOccurrenceId, EarcutSiteId};
use super::prepared_mesh::{
    MeshEdgeRole, MeshProducer, MeshSiteLocation, MeshUseId, PreparedMeshBoundaryUse,
    PreparedMeshFace, PreparedMeshPublication, PreparedMeshSite,
};
use crate::Frame;
use crate::error::Result;
use crate::geometry::embedding_completion::literals::{
    LiteralAddress, LiteralPolygon, LiteralRowLayout, LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::publication::{
    FinishedGeometry, ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, StorageTarget,
    finish_producer_drawing,
};
use crate::geometry::storage::{
    GeometryArrayStorage, PackedStorage, RowSelection, StorageLayout, ValidityMask,
};
use crate::geometry::{
    CoordSeq, Coordinates as _, CsrOffsetBuilder, ExpansionBudget, GeometryErrorKind, HashMap,
    Point, Polygon, PolygonLevel, Polygonal, PublishedTriangleVertices, RingLevel, Shape,
};

pub(in crate::geometry::tessellation) fn prepare_earcut_publication(
    shape: &Shape,
    budget: &mut ExpansionBudget,
) -> Result<PreparedMeshPublication> {
    let mut publication =
        PreparedMeshPublication::new(MeshProducer::Earcut, Vec::new(), Vec::new(), Vec::new());
    let mut source_offset = 0_usize;
    match shape
        .polygonal()
        .ok_or(GeometryErrorKind::PolygonRequired)?
    {
        Polygonal::Polygon(polygon) => {
            append_earcut_polygon(&mut publication, polygon, &mut source_offset, budget)?;
        },
        Polygonal::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                append_earcut_polygon(&mut publication, polygon, &mut source_offset, budget)?;
            }
        },
        Polygonal::Empty => {},
    }
    Ok(publication)
}

fn append_earcut_polygon(
    publication: &mut PreparedMeshPublication,
    polygon: &Polygon,
    source_offset: &mut usize,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let mut source_ranges = Vec::new();
    let mut next_source = *source_offset;
    for ring in polygon.rings() {
        let open_count = ring
            .coord_count()
            .checked_sub(1)
            .expect("admitted polygon rings contain their closing coordinate");
        let source_end = next_source
            .checked_add(open_count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        source_ranges.push(next_source..source_end);
        next_source = source_end;
    }

    let first_face = publication.faces.len();
    let mut occurrences = HashMap::<EarcutOccurrenceId, (EarcutSiteId, usize)>::default();
    let mut emitted_uses =
        HashMap::<(usize, usize), ((EarcutCornerId, EarcutCornerId), MeshUseId)>::default();

    super::earcut::earcut_polygon_with_ids(polygon, &mut |triangle| {
        let local_face = publication.faces.len().wrapping_sub(first_face);
        if triangle.face != local_face {
            return Err(GeometryErrorKind::Invalid(
                "earcut face identities are not in emission order".into(),
            )
            .into());
        }
        budget.check_additional(4)?;
        let face_id = publication.faces.len();
        let [first_point, second_point, third_point] = triangle.points;
        let mut output_sites = [0_usize; 3];
        for (corner_index, (corner, point)) in triangle
            .corners
            .iter()
            .copied()
            .zip(triangle.points.iter().copied())
            .enumerate()
        {
            let source_range = source_ranges.get(corner.site.ring).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "earcut corner is outside its source ring inventory".into(),
                )
            })?;
            let source_ordinal = source_range
                .start
                .checked_add(corner.site.vertex)
                .filter(|&ordinal| ordinal < source_range.end)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "earcut corner is outside its source ring inventory".into(),
                    )
                })?;
            let producer_ordinal = if let Some(&(site, producer_ordinal)) =
                occurrences.get(&corner.occurrence)
            {
                if site != corner.site
                    || publication
                        .sites
                        .get(producer_ordinal)
                        .is_none_or(|prepared| {
                            prepared.source_ordinal != source_ordinal || prepared.record != point
                        })
                {
                    return Err(GeometryErrorKind::Invalid(
                        "earcut occurrence changed its source site".into(),
                    )
                    .into());
                }
                producer_ordinal
            } else {
                let producer_ordinal = publication.sites.len();
                publication.sites.push(PreparedMeshSite::source(
                    producer_ordinal,
                    source_ordinal,
                    point,
                    MeshSiteLocation::Cartesian,
                ));
                occurrences.insert(corner.occurrence, (corner.site, producer_ordinal));
                producer_ordinal
            };
            *output_sites.get_mut(corner_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("earcut face has an invalid corner".into())
            })? = producer_ordinal;
        }

        let output_start = publication.points.len();
        publication.points.reserve(4);
        publication.points.extend_from_slice(&[
            first_point,
            second_point,
            third_point,
            first_point,
        ]);
        let output_end = output_start
            .checked_add(4)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;

        let [first_site, _, _] = output_sites;
        let placeholder = PreparedMeshBoundaryUse::new(
            0,
            MeshUseId::new(face_id, 0),
            first_site,
            first_site,
            None,
            false,
            false,
            MeshEdgeRole::EarcutBoundary,
        );
        let mut boundary_uses = [placeholder; 3];
        let corner_pairs = triangle
            .corners
            .iter()
            .copied()
            .zip(triangle.corners.iter().copied().cycle().skip(1));
        let site_pairs = output_sites
            .iter()
            .copied()
            .zip(output_sites.iter().copied().cycle().skip(1));
        for (side, ((start_corner, end_corner), (start_site, end_site))) in
            corner_pairs.zip(site_pairs).enumerate()
        {
            let use_id = MeshUseId::new(
                face_id,
                u8::try_from(side).expect("an earcut face has three sides"),
            );
            let start_source = publication
                .sites
                .get(start_site)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("earcut side start is outside its site table".into())
                })?
                .source_ordinal;
            let end_source = publication
                .sites
                .get(end_site)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("earcut side end is outside its site table".into())
                })?
                .source_ordinal;
            let side_key = (start_source, end_source);
            let reverse_key = (end_source, start_source);
            let mut role = MeshEdgeRole::EarcutBoundary;
            let reverse = if let Some(((reverse_start, reverse_end), reverse)) =
                emitted_uses.remove(&reverse_key)
            {
                let diagonal = reverse_start == end_corner && reverse_end == start_corner;
                let bridge = reverse_start.site == end_corner.site
                    && reverse_end.site == start_corner.site
                    && reverse_start.occurrence != end_corner.occurrence
                    && reverse_end.occurrence != start_corner.occurrence;
                if !diagonal && !bridge {
                    return Err(GeometryErrorKind::Invalid(
                        "earcut reciprocal sides do not share selected occurrences or bridge sites"
                            .into(),
                    )
                    .into());
                }
                let paired_role = if bridge {
                    MeshEdgeRole::EarcutBridge
                } else {
                    MeshEdgeRole::EarcutDiagonal
                };
                let previous_face = publication.faces.get_mut(reverse.face).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "earcut reciprocal use references an unknown face".into(),
                    )
                })?;
                let previous = previous_face
                    .boundary_uses
                    .get_mut(usize::from(reverse.side))
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "earcut reciprocal use references an unknown side".into(),
                        )
                    })?;
                if previous.reverse.is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "earcut side has more than one reciprocal use".into(),
                    )
                    .into());
                }
                previous.reverse = Some(use_id);
                previous.role = paired_role;
                role = paired_role;
                Some(reverse)
            } else {
                if emitted_uses
                    .insert(side_key, ((start_corner, end_corner), use_id))
                    .is_some()
                {
                    return Err(GeometryErrorKind::Invalid(
                        "earcut emits a directed side more than once".into(),
                    )
                    .into());
                }
                None
            };
            *boundary_uses.get_mut(side).ok_or_else(|| {
                GeometryErrorKind::Invalid("earcut face has an invalid side".into())
            })? = PreparedMeshBoundaryUse::new(
                u8::try_from(side).expect("an earcut face has three sides"),
                use_id,
                start_site,
                end_site,
                reverse,
                false,
                false,
                role,
            );
        }
        publication.faces.push(PreparedMeshFace::new(
            face_id,
            output_sites,
            output_sites,
            [0, 1, 2],
            false,
            output_start..output_end,
            &boundary_uses,
        ));
        budget.add(4)?;
        Ok(())
    })?;
    *source_offset = next_source;
    Ok(())
}

pub(in crate::geometry) fn mesh_publication_array_receipt(
    published: PublishedTriangleVertices,
) -> Result<ProducerDrawingReceipt> {
    use std::sync::Arc;

    use crate::work::Unmetered;

    let mut points = Vec::new();
    let mut ring_ends = Vec::new();
    let mut row_map = Vec::new();
    let mut literals = Vec::new();
    let mut next_image = 0_usize;
    append_mesh_outputs(
        published,
        None,
        &mut points,
        &mut ring_ends,
        &mut row_map,
        &mut literals,
        &mut next_image,
    )?;
    let storage = mesh_triangle_storage(&points, &ring_ends)?;
    let mut work = Unmetered;
    let buffers = FrozenLiteralBuffers::rows(
        Arc::new(storage),
        None,
        StorageLayout::LogicalRows,
        row_map,
        &mut work,
    )?;
    Ok(ProducerDrawingReceipt::new(buffers, literals))
}

pub(crate) fn finish_mesh_publication_array(
    published: PublishedTriangleVertices,
    frame: Frame,
) -> Result<FinishedGeometry> {
    let receipt = mesh_publication_array_receipt(published)?;
    finish_producer_drawing(receipt, frame, StorageTarget::Array)
}

pub(crate) fn finish_mesh_publication_groups(
    rows: Vec<Option<PublishedTriangleVertices>>,
    frame: Frame,
    missing: Option<ValidityMask>,
) -> Result<FinishedGeometry> {
    use std::sync::Arc;

    use crate::work::Unmetered;

    if missing
        .as_ref()
        .is_some_and(|mask| mask.len() != rows.len())
    {
        return Err(GeometryErrorKind::Invalid(
            "mesh group validity does not match its logical rows".into(),
        )
        .into());
    }

    let mut points = Vec::new();
    let mut ring_ends = Vec::new();
    let mut value_map = Vec::new();
    let mut literals = Vec::new();
    let mut offsets = Vec::with_capacity(
        rows.len()
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?,
    );
    let mut next_image = 0_usize;
    offsets.push(0_i64);
    for (group_row, published) in rows.into_iter().enumerate() {
        let is_missing = missing
            .as_ref()
            .is_some_and(|mask| mask.is_missing(group_row));
        if is_missing != published.is_none() {
            return Err(GeometryErrorKind::Invalid(
                "mesh group receipt does not match its source-row validity".into(),
            )
            .into());
        }
        if let Some(published) = published {
            append_mesh_outputs(
                published,
                Some(group_row),
                &mut points,
                &mut ring_ends,
                &mut value_map,
                &mut literals,
                &mut next_image,
            )?;
        }
        offsets.push(
            i64::try_from(value_map.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        );
    }

    let storage = mesh_triangle_storage(&points, &ring_ends)?;
    let mut work = Unmetered;
    let buffers = FrozenLiteralBuffers::groups(
        Arc::new(storage),
        None,
        StorageLayout::LogicalRows,
        value_map,
        offsets,
        missing,
        &mut work,
    )?;
    let receipt = ProducerDrawingReceipt::new(buffers, literals);
    finish_producer_drawing(receipt, frame, StorageTarget::Groups)
}

fn append_mesh_outputs(
    published: PublishedTriangleVertices,
    group_row: Option<usize>,
    points: &mut Vec<Point>,
    ring_ends: &mut Vec<usize>,
    row_map: &mut Vec<Option<LiteralRowLayout<ProducerImageId>>>,
    literals: &mut Vec<ProducerLiteral>,
    next_image: &mut usize,
) -> Result<()> {
    validate_published_mesh(&published)?;
    if group_row.is_none() && !row_map.is_empty() {
        return Err(GeometryErrorKind::Invalid(
            "a scalar mesh array receipt was appended to existing rows".into(),
        )
        .into());
    }
    let output_base = points.len();
    let mut source_points = published.points;
    let source_ring_ends = published.ring_ends;
    let mut start = 0_usize;
    for &end in &source_ring_ends {
        let ring = source_points.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("mesh triangle range is outside its published points".into())
        })?;
        let first = ring.first().copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("published mesh triangle has no ring points".into())
        })?;
        if ring.iter().any(|point| point.axes != first.axes) {
            return Err(GeometryErrorKind::Invalid(
                "published mesh triangle changes coordinate axes within one ring".into(),
            )
            .into());
        }

        let value_row = row_map.len();
        let logical_row = group_row.unwrap_or(value_row);
        let value_row_address = group_row.map(|_| value_row);
        let address = LiteralAddress {
            operation: 0,
            logical_row,
            group_row,
            value_row: value_row_address,
            output: 0,
        };
        let mut images = Vec::with_capacity(ring.len());
        for &point in ring {
            let image = ProducerImageId::new(*next_image);
            *next_image = (*next_image)
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            images.push(image);
            literals.push(ProducerLiteral::from_point(address, image, point));
        }
        let sequence = LiteralSequence {
            output: 0,
            axes: first.axes,
            images,
        };
        row_map.push(Some(LiteralRowLayout {
            operation: 0,
            logical_row,
            group_row,
            value_row: value_row_address,
            layout: ShapeLiteralLayout {
                structure: Some(
                    crate::geometry::embedding_completion::literals::ShapeStructure::primitive(
                        crate::geometry::GeometryKind::Polygon,
                        first.axes,
                        false,
                    ),
                ),
                points: Vec::new(),
                lines: Vec::new(),
                polygons: vec![LiteralPolygon {
                    output: 0,
                    axes: first.axes,
                    shell: sequence,
                    holes: Vec::new(),
                }],
            },
        }));
        ring_ends.push(
            output_base
                .checked_add(end)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
        );
        start = end;
    }
    if start != source_points.len() {
        return Err(GeometryErrorKind::Invalid(
            "mesh triangle ranges leave published points unused".into(),
        )
        .into());
    }
    points.append(&mut source_points);
    Ok(())
}

fn validate_published_mesh(published: &PublishedTriangleVertices) -> Result<()> {
    let mesh = published.mesh_publication.as_ref().ok_or_else(|| {
        GeometryErrorKind::Invalid("geographic mesh output lost its prepared records".into())
    })?;
    if mesh.faces.len() != published.ring_ends.len() {
        return Err(GeometryErrorKind::Invalid(
            "published mesh rings differ from their prepared face map".into(),
        )
        .into());
    }
    let site_locations_match = mesh.sites.iter().enumerate().all(|(ordinal, site)| {
        site.producer_ordinal == ordinal
            && matches!(
                (mesh.producer, site.location),
                (MeshProducer::Delaunay, MeshSiteLocation::Retained(_))
                    | (MeshProducer::Earcut, MeshSiteLocation::Cartesian)
            )
    });
    if !site_locations_match {
        return Err(GeometryErrorKind::Invalid(
            "prepared mesh producer disagrees with its site locations".into(),
        )
        .into());
    }

    let mut ring_start = 0_usize;
    let mut source_start = 0_usize;
    for (&ring_end, face) in published.ring_ends.iter().zip(&mesh.faces) {
        let ring_len = ring_end.checked_sub(ring_start).ok_or_else(|| {
            GeometryErrorKind::Invalid("published mesh ring offsets are decreasing".into())
        })?;
        let source_end = source_start
            .checked_add(4)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if ring_len < 4 || face.output_vertices != (source_start..source_end) {
            return Err(GeometryErrorKind::Invalid(
                "published mesh ring lost its prepared face order".into(),
            )
            .into());
        }
        let site_pairs = face
            .output_sites
            .iter()
            .copied()
            .zip(face.output_sites.iter().copied().cycle().skip(1));
        for (side, (boundary, (start_site, end_site))) in
            face.boundary_uses.iter().zip(site_pairs).enumerate()
        {
            let selected_side = face.output_to_selected.get(side).copied();
            let selected_site =
                selected_side.and_then(|selected| face.selected_sites.get(usize::from(selected)));
            let role_matches = matches!(
                (mesh.producer, boundary.role),
                (MeshProducer::Delaunay, MeshEdgeRole::Delaunay)
                    | (
                        MeshProducer::Earcut,
                        MeshEdgeRole::EarcutBoundary
                            | MeshEdgeRole::EarcutDiagonal
                            | MeshEdgeRole::EarcutBridge
                    )
            );
            let twin_matches_role = matches!(
                (boundary.role, boundary.reverse),
                (MeshEdgeRole::Delaunay, _)
                    | (MeshEdgeRole::EarcutBoundary, None)
                    | (
                        MeshEdgeRole::EarcutDiagonal | MeshEdgeRole::EarcutBridge,
                        Some(_)
                    )
            );
            if usize::from(boundary.output_side) != side
                || boundary.selected_use.face != face.selected_face
                || selected_side != Some(boundary.selected_use.side)
                || boundary.reversed_for_output != face.reversed
                || !role_matches
                || !twin_matches_role
                || selected_site != face.output_sites.get(side)
                || face.output_sites.get(side) != Some(&start_site)
                || start_site != boundary.start_site
                || end_site != boundary.end_site
                || mesh.sites.get(boundary.start_site).is_none()
                || mesh.sites.get(boundary.end_site).is_none()
            {
                return Err(GeometryErrorKind::Invalid(
                    "prepared mesh side no longer matches its selected face".into(),
                )
                .into());
            }
        }
        source_start = source_end;
        ring_start = ring_end;
    }
    if ring_start != published.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "published mesh face map does not cover its point stream".into(),
        )
        .into());
    }
    Ok(())
}

fn mesh_triangle_storage(points: &[Point], ring_ends: &[usize]) -> Result<GeometryArrayStorage> {
    use std::sync::Arc;

    let Some(first) = points.first() else {
        if !ring_ends.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "empty mesh points have nonempty ring offsets".into(),
            )
            .into());
        }
        return Ok(GeometryArrayStorage::Mixed(Vec::new()));
    };
    let axes = first.axes;
    if points.iter().any(|point| point.axes != axes) {
        let shapes = Shape::triangle_shapes_from_published_rings(points, ring_ends)?;
        return Ok(GeometryArrayStorage::Mixed(shapes));
    }

    let mut ring_offsets = CsrOffsetBuilder::with_capacity(ring_ends.len());
    let mut polygon_offsets = CsrOffsetBuilder::with_capacity(ring_ends.len());
    for (index, &end) in ring_ends.iter().enumerate() {
        ring_offsets.push_end(end, points.len())?;
        polygon_offsets.push_end(index.wrapping_add(1), ring_ends.len())?;
    }
    let ring_offsets = ring_offsets.finish(points.len())?.cast_level::<RingLevel>();
    let polygon_offsets = polygon_offsets
        .finish(ring_ends.len())?
        .cast_level::<PolygonLevel>();
    Ok(GeometryArrayStorage::Packed(PackedStorage::Polygons {
        coords: Arc::new(CoordSeq::try_from_points(points)?),
        ring_offsets,
        polygon_offsets,
        row_map: RowSelection::Identity,
    }))
}

#[cfg(test)]
mod tests {
    use super::prepare_earcut_publication;
    use crate::geometry::tessellation::{MeshEdgeRole, MeshProducer};
    use crate::geometry::{ExpansionBudget, MOrdinate, Point, Polygon, Ring, Shape, ZOrdinate};

    fn xy(x: f64, y: f64) -> Point {
        Point::new(x, y).expect("finite test coordinate")
    }

    fn xyz(x: f64, y: f64, z: f64) -> Point {
        Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None)).expect("finite test coordinate")
    }

    fn polygon(shell: Vec<Point>, holes: Vec<Vec<Point>>) -> Polygon {
        Polygon::new(
            Ring::closed(shell).expect("valid test shell"),
            holes
                .into_iter()
                .map(|hole| Ring::closed(hole).expect("valid test hole"))
                .collect(),
        )
    }

    #[test]
    fn earcut_mesh_publication_keeps_face_sides_and_bridge_twins() {
        let source = Shape::Polygon(polygon(
            vec![
                xyz(0.0, 0.0, 1.0),
                xyz(4.0, 0.0, 2.0),
                xyz(8.0, 0.0, 3.0),
                xyz(8.0, 8.0, 4.0),
                xyz(0.0, 8.0, 5.0),
                xyz(0.0, 0.0, 1.0),
            ],
            vec![vec![
                xy(2.0, 2.0),
                xy(2.0, 4.0),
                xy(4.0, 4.0),
                xy(4.0, 2.0),
                xy(2.0, 2.0),
            ]],
        ));
        let mut budget = ExpansionBudget::new("triangulate", "method");
        let publication = prepare_earcut_publication(&source, &mut budget)
            .expect("public Earcut selection has a prepared mesh product");

        assert_eq!(publication.producer, MeshProducer::Earcut);
        assert_eq!(publication.faces.len(), 8);
        assert_eq!(publication.points.len(), publication.faces.len() * 4);
        let output_shapes = publication
            .faces
            .iter()
            .map(|face| {
                let [a, b, c, _] = publication
                    .points
                    .get(face.output_vertices.clone())
                    .and_then(|ring| ring.first_chunk::<4>())
                    .copied()
                    .expect("each prepared face owns its closed four-point ring");
                super::super::earcut::try_triangle_shape(a, b, c)
                    .expect("published ears retain their per-face common axes")
            })
            .collect::<Vec<_>>();
        assert_eq!(output_shapes.len(), publication.faces.len());
        let mut bridge_uses = 0;
        let mut diagonal_uses = 0;
        for (face_index, (face, output_shape)) in
            publication.faces.iter().zip(&output_shapes).enumerate()
        {
            assert_eq!(face.selected_face, face_index);
            assert_eq!(face.output_vertices, face_index * 4..face_index * 4 + 4);
            assert_eq!(face.output_to_selected, [0, 1, 2]);
            assert!(!face.reversed);
            assert_eq!(
                output_shape.axes().has_z(),
                face.output_sites.iter().all(|&site| {
                    publication
                        .sites
                        .get(site)
                        .expect("selected face site belongs to the occurrence table")
                        .record
                        .axes
                        .has_z()
                })
            );
            let output = publication
                .points
                .get(face.output_vertices.clone())
                .expect("each selected face owns its emitted ring");
            assert_eq!(output.len(), 4);
            assert_eq!(output.first(), output.last());
            for (side, boundary) in face.boundary_uses.iter().enumerate() {
                assert_eq!(usize::from(boundary.output_side), side);
                assert_eq!(boundary.selected_use.face, face.selected_face);
                assert_eq!(
                    boundary.start_site,
                    *face.output_sites.get(side).expect("three face corners")
                );
                assert_eq!(
                    boundary.end_site,
                    *face
                        .output_sites
                        .get(side.wrapping_add(1) % 3)
                        .expect("three face corners")
                );
                match boundary.role {
                    MeshEdgeRole::EarcutBoundary => assert!(boundary.reverse.is_none()),
                    MeshEdgeRole::EarcutDiagonal | MeshEdgeRole::EarcutBridge => {
                        let reverse = boundary
                            .reverse
                            .expect("every selected internal side has its reciprocal use");
                        let reciprocal = publication
                            .faces
                            .get(reverse.face)
                            .and_then(|face| face.boundary_uses.get(usize::from(reverse.side)))
                            .expect("reciprocal use belongs to a selected face");
                        assert_eq!(reciprocal.reverse, Some(boundary.selected_use));
                        assert_eq!(reciprocal.role, boundary.role);
                        let start = publication
                            .sites
                            .get(boundary.start_site)
                            .expect("side start belongs to the occurrence table");
                        let end = publication
                            .sites
                            .get(boundary.end_site)
                            .expect("side end belongs to the occurrence table");
                        let reverse_start = publication
                            .sites
                            .get(reciprocal.start_site)
                            .expect("twin start belongs to the occurrence table");
                        let reverse_end = publication
                            .sites
                            .get(reciprocal.end_site)
                            .expect("twin end belongs to the occurrence table");
                        assert_eq!(reverse_start.source_ordinal, end.source_ordinal);
                        assert_eq!(reverse_end.source_ordinal, start.source_ordinal);
                        match boundary.role {
                            MeshEdgeRole::EarcutDiagonal => diagonal_uses += 1,
                            MeshEdgeRole::EarcutBridge => bridge_uses += 1,
                            MeshEdgeRole::Delaunay | MeshEdgeRole::EarcutBoundary => {
                                unreachable!("the twin roles were matched above")
                            },
                        }
                    },
                    MeshEdgeRole::Delaunay => {
                        unreachable!("Earcut does not issue Delaunay side roles")
                    },
                }
            }
        }
        assert_eq!(bridge_uses, 2, "one bridge has two directed occurrences");
        assert_eq!(
            diagonal_uses, 14,
            "seven triangulation diagonals have twins"
        );
    }
}
