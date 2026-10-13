fn ring_edges(n: usize) -> Vec<[usize; 2]> {
    (0..n)
        .map(|i| [i, i.wrapping_add(1).rem_euclid(n)])
        .collect()
}
fn inside_count(cdt: &ConstrainedMesh) -> usize {
    let inside = cdt.inside_faces();
    cdt.mesh
        .face_ids()
        .zip(inside)
        .filter(|(face, kept)| cdt.mesh.alive(*face) && *kept)
        .count()
}
