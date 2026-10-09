use crate::geometry::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, HashMap, LineSeq, Point, PointKey, Polygon, Shape,
    Work,
};
use crate::geometry::segments::try_sort_by_with_work;

fn extend_chain(edge: &CoordSeq, from: PointKey, skip: usize, out: &mut CoordSeqBuilder) {
    if PointKey::new(edge.first().expect("edge has >= 2 vertices")) == from {
        out.extend_window(edge, skip..edge.len());
    } else {
        for point in edge.iter().rev().skip(skip) {
            out.push(point);
        }
    }
}

/// Dissolve linework edges into maximal chains, split only at genuine
/// junctions (nodes of degree other than two). Every edge is consumed once;
/// output order follows the lowest unused seed, so it is independent of
/// hash iteration. Node incidence is a CSR over the distinct end keys: one
/// map lookup per edge end and no per-node allocation, which is what an
/// overlay of thousands of atomic segments spends its time on otherwise.
pub(crate) fn merge_chains<E: AsRef<CoordSeq>>(edges: &[E]) -> crate::error::Result<Vec<CoordSeq>> {
    let mut ends: Vec<[PointKey; 2]> = Vec::with_capacity(edges.len());
    let mut live: Vec<usize> = Vec::with_capacity(edges.len());
    for (index, edge) in edges.iter().enumerate() {
        let edge = edge.as_ref();
        let mut points = edge.iter();
        let Some(first) = points.next().map(PointKey::new) else {
            continue;
        };
        if points.any(|point| PointKey::new(point) != first) {
            ends.push([
                first,
                PointKey::new(edge.last().expect("edge has >= 2 vertices")),
            ]);
            live.push(index);
        }
    }
    if live.len() <= 1 {
        let mut result = Vec::with_capacity(live.len());
        for index in live {
            let edge = edges
                .get(index)
                .expect("live edge ordinal comes from the input edges")
                .as_ref();
            let mut coords = CoordSeqBuilder::with_capacity(edge.axes(), edge.len());
            extend_chain(edge, ends[0][0], 0, &mut coords);
            result.push(coords.finish()?);
        }
        return Ok(result);
    }
    let mut node_of: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    node_of.reserve(ends.len());
    let mut degree: Vec<u32> = Vec::new();
    let node_capacity = ends
        .len()
        .checked_mul(2)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    degree.reserve(node_capacity);
    let mut node_ends: Vec<[u32; 2]> = Vec::with_capacity(ends.len());
    for pair in &ends {
        let mut ids = [0_u32; 2];
        for (slot, key) in pair.iter().enumerate() {
            let next = u32::try_from(degree.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
            let id = *node_of.entry(*key).or_insert(next);
            if id == next {
                degree.push(0);
            }
            degree[id as usize] = degree[id as usize]
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ids[slot] = id;
        }
        node_ends.push(ids);
    }
    drop(node_of);
    let offset_capacity = degree
        .len()
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut offsets: Vec<u32> = Vec::with_capacity(offset_capacity);
    offsets.push(0);
    for &count in &degree {
        let next = offsets[offsets.len() - 1]
            .checked_add(count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        offsets.push(next);
    }
    let mut cursor = Vec::with_capacity(offsets.len());
    cursor.extend_from_slice(&offsets);
    let slot_count = usize::try_from(*offsets.last().expect("offsets has an initial zero"))
        .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    let mut slots = vec![0_u32; slot_count];
    for (edge, ids) in node_ends.iter().enumerate() {
        for &id in ids {
            slots[cursor[id as usize] as usize] = edge as u32;
            cursor[id as usize] += 1;
        }
    }
    drop(cursor);
    let incident =
        |node: u32| &slots[offsets[node as usize] as usize..offsets[node as usize + 1] as usize];

    let mut used = Vec::with_capacity(ends.len());
    used.resize(ends.len(), false);
    let mut chains: Vec<CoordSeq> = Vec::with_capacity(ends.len());
    let walk =
        |seed: usize, start_slot: usize, used: &mut [bool]| -> crate::error::Result<CoordSeq> {
            let first = edges
                .get(
                    *live
                        .get(seed)
                        .expect("chain seed belongs to the live edges"),
                )
                .expect("live edge ordinal comes from the input edges")
                .as_ref();
            let mut coords = CoordSeqBuilder::with_capacity(first.axes(), first.len());
            let mut edge = seed;
            let mut from = node_ends[seed][start_slot];
            loop {
                used[edge] = true;
                let [a, b] = node_ends[edge];
                let from_key = ends[edge][usize::from(a != from)];
                extend_chain(
                    edges[live[edge]].as_ref(),
                    from_key,
                    usize::from(coords.len() != 0),
                    &mut coords,
                );
                let far = if a == from { b } else { a };
                if degree[far as usize] != 2 {
                    break;
                }
                match incident(far)
                    .iter()
                    .copied()
                    .find(|&next| next as usize != edge && !used[next as usize])
                {
                    Some(next) => {
                        edge = next as usize;
                        from = far;
                    },
                    None => break,
                }
            }
            coords.finish()
        };
    // Open chains first: every edge with a barrier end (degree != 2) starts
    // a chain that runs until the next barrier; edges interior to such a
    // chain are consumed mid-walk and skipped when their own seed turn comes.
    for seed in 0..ends.len() {
        if used[seed] {
            continue;
        }
        if let Some(slot) = (0..2).find(|&slot| degree[node_ends[seed][slot] as usize] != 2) {
            chains.push(walk(seed, slot, &mut used)?);
        }
    }
    // Whatever remains is an all-degree-2 cycle; walk each from its lowest
    // unused edge.
    for seed in 0..ends.len() {
        if !used[seed] {
            chains.push(walk(seed, 0, &mut used)?);
        }
    }
    Ok(chains)
}

/// Serialize an already certified exact-partition plan without reclassifying
/// points or lines against rounded polygon coordinates. The exact selector has
/// already performed dimensional absorption, so any further coverage test here
/// would be a second topology owner.
pub(crate) fn build_certified_overlay_shape(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
) -> crate::error::Result<Shape> {
    build_certified_overlay_shape_with_axes(points, lines, polygons, CoordinateAxes::XY)
}

pub(crate) fn build_certified_overlay_shape_with_axes(
    mut points: Vec<Point>,
    mut lines: Vec<CoordSeq>,
    mut polygons: Vec<Polygon>,
    axes: CoordinateAxes,
) -> crate::error::Result<Shape> {
    points.sort_unstable_by(crate::geometry::compare_points);
    lines.sort_unstable_by(crate::geometry::compare_point_slices);
    polygons.sort_unstable_by(crate::geometry::compare_polygons);
    assemble_sorted_overlay_shape(points, lines, polygons, axes)
}

pub(crate) struct CertifiedOverlayOrder {
    pub(crate) points: Vec<usize>,
    pub(crate) lines: Vec<usize>,
    pub(crate) polygons: Vec<usize>,
}

pub(crate) fn build_certified_overlay_shape_with_axes_and_order<W: Work + ?Sized>(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
    axes: CoordinateAxes,
    work: &mut W,
) -> crate::error::Result<(Shape, CertifiedOverlayOrder)> {
    work.charge(
        u64::try_from(points.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut ordered_points = Vec::with_capacity(points.len());
    for (index, point) in points.into_iter().enumerate() {
        work.charge(1)?;
        ordered_points.push((index, point));
    }
    try_sort_by_with_work(&mut ordered_points, work, |left, right, work| {
        work.charge(1)?;
        Ok(crate::geometry::compare_points(&left.1, &right.1).then_with(|| left.0.cmp(&right.0)))
    })?;
    work.charge(
        u64::try_from(lines.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut ordered_lines = Vec::with_capacity(lines.len());
    for (index, line) in lines.into_iter().enumerate() {
        work.charge(1)?;
        ordered_lines.push((index, line));
    }
    try_sort_by_with_work(&mut ordered_lines, work, |left, right, work| {
        work.charge(1)?;
        Ok(
            crate::geometry::compare_point_slices(&left.1, &right.1)
                .then_with(|| left.0.cmp(&right.0)),
        )
    })?;
    work.charge(
        u64::try_from(polygons.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut ordered_polygons = Vec::with_capacity(polygons.len());
    for (index, polygon) in polygons.into_iter().enumerate() {
        work.charge(1)?;
        ordered_polygons.push((index, polygon));
    }
    try_sort_by_with_work(&mut ordered_polygons, work, |left, right, work| {
        work.charge(1)?;
        Ok(crate::geometry::compare_polygons(&left.1, &right.1)
            .then_with(|| left.0.cmp(&right.0)))
    })?;
    let ordered_count = ordered_points
        .len()
        .checked_add(ordered_lines.len())
        .and_then(|count| count.checked_add(ordered_polygons.len()))
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    work.charge(
        u64::try_from(ordered_count).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
    )?;
    let mut point_order = Vec::with_capacity(ordered_points.len());
    let mut points = Vec::with_capacity(ordered_points.len());
    for (index, point) in ordered_points {
        work.charge(1)?;
        point_order.push(index);
        points.push(point);
    }
    let mut line_order = Vec::with_capacity(ordered_lines.len());
    let mut lines = Vec::with_capacity(ordered_lines.len());
    for (index, line) in ordered_lines {
        work.charge(1)?;
        line_order.push(index);
        lines.push(line);
    }
    let mut polygon_order = Vec::with_capacity(ordered_polygons.len());
    let mut polygons = Vec::with_capacity(ordered_polygons.len());
    for (index, polygon) in ordered_polygons {
        work.charge(1)?;
        polygon_order.push(index);
        polygons.push(polygon);
    }
    let shape = assemble_sorted_overlay_shape(points, lines, polygons, axes)?;
    Ok((
        shape,
        CertifiedOverlayOrder {
            points: point_order,
            lines: line_order,
            polygons: polygon_order,
        },
    ))
}

fn assemble_sorted_overlay_shape(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
    axes: CoordinateAxes,
) -> crate::error::Result<Shape> {
    let point_part = (!points.is_empty())
        .then(|| Shape::narrow_points(axes, points))
        .transpose()?;
    let line_part = if lines.is_empty() {
        None
    } else {
        let mut proven = Vec::with_capacity(lines.len());
        for line in lines {
            proven.push(LineSeq::from_proven(line));
        }
        Some(Shape::narrow_lines(axes, proven))
    };
    let polygon_part = (!polygons.is_empty()).then(|| Shape::narrow_polygons(axes, polygons));
    let mut parts = Vec::with_capacity(3);
    parts.extend([point_part, line_part, polygon_part].into_iter().flatten());
    match <[Shape; 1]>::try_from(parts) {
        Ok([single]) => Ok(single),
        Err(parts) => Shape::geometry_collection(parts),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certified_overlay_order_tracks_sorted_point_slots() -> crate::error::Result<()> {
        let input = [
            Point::new_raw_xy(2.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
        ];
        let mut work = Unmetered;
        let (shape, order) = build_certified_overlay_shape_with_axes_and_order(
            input.to_vec(),
            Vec::new(),
            Vec::new(),
            CoordinateAxes::XY,
            &mut work,
        )?;
        assert_eq!(order.points, [1, 0]);
        let Shape::MultiPoint(points) = shape else {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "two certified points did not produce a MultiPoint".into(),
            )
            .into());
        };
        assert_eq!(points.point_at(0).x, 1.0);
        assert_eq!(points.point_at(1).x, 2.0);
        Ok(())
    }
}
