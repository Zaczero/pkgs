use super::*;

#[cfg(test)]
pub(crate) fn cleaned_earcut_ring<C: Coordinates + ?Sized>(
    ring: &C,
    exterior_cw: bool,
) -> Result<Vec<Point>> {
    let mut next_occurrence = 0_usize;
    cleaned_earcut_ring_with_ids(ring, exterior_cw, 0, &mut next_occurrence)
        .map(|(points, _)| points)
}

pub(super) fn cleaned_earcut_ring_with_ids<C: Coordinates + ?Sized>(
    ring: &C,
    exterior_cw: bool,
    source_ring: usize,
    next_occurrence: &mut usize,
) -> Result<(Vec<Point>, Vec<EarcutCornerId>)> {
    let mut vertices = open_ring(ring);
    let mut identities = source_identities(source_ring, vertices.len(), next_occurrence)?;
    dedup_circular_points_with_ids(&mut vertices, &mut identities);
    strip_collinear_vertices_with_ids(&mut vertices, Some(&mut identities));
    orient_earcut_ring_with_ids(&mut vertices, Some(&mut identities), exterior_cw);
    validate_earcut_ring(&vertices)?;
    Ok((vertices, identities))
}

#[cfg(test)]
pub(crate) fn orient_earcut_ring(vertices: &mut [Point], exterior_cw: bool) {
    orient_earcut_ring_with_ids(vertices, None, exterior_cw);
}

fn orient_earcut_ring_with_ids(
    vertices: &mut [Point],
    identities: Option<&mut Vec<EarcutCornerId>>,
    exterior_cw: bool,
) {
    if let Some(identities) = identities.as_deref() {
        debug_assert_eq!(vertices.len(), identities.len());
    }
    if vertices.len() < 3 {
        return;
    }
    let is_clockwise = open_point_cycle_winding(vertices) == RingWinding::Clockwise;
    if is_clockwise != exterior_cw {
        vertices.reverse();
        if let Some(identities) = identities {
            identities.reverse();
        }
    }
}

fn dedup_circular_points_with_ids(vertices: &mut Vec<Point>, identities: &mut Vec<EarcutCornerId>) {
    debug_assert_eq!(vertices.len(), identities.len());
    let mut write = 0_usize;
    for read in 0..vertices.len() {
        let read_vertex = *vertices
            .get(read)
            .expect("the read cursor comes from the vertex length");
        if write != 0
            && vertices
                .get(write.wrapping_sub(1))
                .is_some_and(|&previous| same_point(previous, read_vertex))
        {
            continue;
        }
        let identity = identities
            .get(read)
            .copied()
            .expect("source identities stay parallel with the vertices");
        *vertices
            .get_mut(write)
            .expect("the write cursor never advances past the read cursor") = read_vertex;
        *identities
            .get_mut(write)
            .expect("source identities stay parallel with the vertices") = identity;
        write = write.wrapping_add(1);
    }
    vertices.truncate(write);
    identities.truncate(write);
    while vertices.len() > 1
        && same_point(
            *vertices.first().expect("checked non-empty"),
            *vertices.last().expect("checked non-empty"),
        )
    {
        vertices.pop();
        identities.pop();
    }
}

#[cfg(test)]
pub(crate) fn strip_collinear_vertices(vertices: &mut Vec<Point>) {
    strip_collinear_vertices_with_ids(vertices, None);
}

fn strip_collinear_vertices_with_ids(
    vertices: &mut Vec<Point>,
    mut identities: Option<&mut Vec<EarcutCornerId>>,
) {
    if vertices.len() < 3 {
        return;
    }
    if let Some(identities) = identities.as_deref() {
        debug_assert_eq!(vertices.len(), identities.len());
    }
    let source_identities = identities.as_deref_mut().map(std::mem::take);
    let is_collinear = |a: Point, b: Point, c: Point| -> bool {
        orientation(a, b, c) == Orientation::Collinear && point_on_segment(b, a, c)
    };
    // O(n) single pass with a write-head stack (was an O(n²) `Vec::remove`
    // loop — quadratic for densified/segmentized rings with many collinear
    // vertices). A collinear-run's interior points collapse to its endpoints,
    // and that fixpoint is unique, so this keeps the EXACT same vertex set:
    // after pushing a vertex, while the last three are collinear, drop the
    // middle by overwriting it with the last and popping (O(1), no shift).
    let mut out: Vec<Point> = Vec::with_capacity(vertices.len());
    let mut out_identities = source_identities
        .as_ref()
        .map(|_| Vec::with_capacity(vertices.len()));
    for (index, &curr) in vertices.iter().enumerate() {
        out.push(curr);
        if let (Some(input_ids), Some(output_ids)) =
            (source_identities.as_ref(), &mut out_identities)
        {
            output_ids.push(
                *input_ids
                    .get(index)
                    .expect("source identities stay parallel with the vertices"),
            );
        }
        loop {
            match out.as_mut_slice() {
                [.., first, middle, last] if is_collinear(*first, *middle, *last) => {
                    *middle = *last;
                    out.pop();
                    if let Some(output_ids) = &mut out_identities {
                        let last_id = *output_ids.last().expect("collinear output has a last ID");
                        let middle = output_ids
                            .len()
                            .checked_sub(2)
                            .expect("a collinear output triple has a middle ID");
                        *output_ids
                            .get_mut(middle)
                            .expect("the middle ID remains in the output prefix") = last_id;
                        output_ids.pop();
                    }
                },
                _ => break,
            }
        }
    }
    // Seam fixup: the ring is closed, so re-check the wrap-around triples
    // (…, last, first) and (last, first, second), collapsing the middle until
    // stable. A simple polygon's seam holds O(1) collinear vertices.
    loop {
        let removes_last = match out.as_slice() {
            [first, .., second_last, last] => is_collinear(*second_last, *last, *first),
            _ => false,
        };
        if removes_last {
            out.pop();
            if let Some(output_ids) = &mut out_identities {
                output_ids.pop();
            }
            continue;
        }
        let removes_first = match out.as_slice() {
            [first, second, .., last] => is_collinear(*last, *first, *second),
            _ => false,
        };
        if removes_first {
            out.remove(0);
            if let Some(output_ids) = &mut out_identities {
                output_ids.remove(0);
            }
        } else {
            break;
        }
    }
    *vertices = out;
    if let (Some(identities), Some(out_identities)) = (identities, out_identities) {
        *identities = out_identities;
    }
}

pub(super) fn source_identities(
    source_ring: usize,
    vertex_count: usize,
    next_occurrence: &mut usize,
) -> Result<Vec<EarcutCornerId>> {
    let mut identities = Vec::with_capacity(vertex_count);
    for vertex in 0..vertex_count {
        let occurrence = *next_occurrence;
        *next_occurrence = next_occurrence
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        identities.push(EarcutCornerId::source(source_ring, vertex, occurrence));
    }
    Ok(identities)
}

#[cfg(test)]
pub(crate) fn filter_earcut_ring_points(ring: &mut Vec<Point>) {
    dedup_consecutive_points(ring);
    strip_collinear_vertices(ring);
}

pub(crate) fn open_ring<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    let len = points.coord_count();
    let closed = len > 1 && same_point(points.nth_coord(0), points.nth_coord(len.wrapping_sub(1)));
    let keep = if closed { len.wrapping_sub(1) } else { len };
    points.iter_coords().take(keep).collect()
}

pub(crate) fn closed_ring<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    let mut ring = points.iter_coords().collect::<Vec<_>>();
    if let Some(first) = ring.first().copied() {
        ring.push(first);
    }
    ring
}
